# Adversarial multi-instance tests for cross-process repo locking and repo quota release.
# Topology mirrors an unsupported-but-runnable deployment: several zot instances, one S3
# bucket, one shared redis cache, no cluster block, round-robin load balancer. GC runs
# hot (1s delay, 2s interval) so sweeps interleave with the request path.

NUM_ZOT_INSTANCES=3
ZOT_LOG_DIR=/tmp/zot-ft-logs/locking

load helpers_zot
load helpers_cloud
load helpers_haproxy
load helpers_redis
load ../port_helper

function verify_prerequisites() {
    if [ ! $(command -v curl) ] || [ ! $(command -v jq) ] || [ ! $(command -v skopeo) ]; then
        echo "you need curl, jq and skopeo as prerequisites to running the tests" >&3
        return 1
    fi

    return 0
}

function create_locking_config_file() {
    local zot_server_address=${1}
    local zot_server_port=${2}
    local zot_root_dir=${3}
    local zot_config_file=${4}
    local zot_log_file=${5}
    local redis_url=${6}

    cat > ${zot_config_file} <<EOF
{
    "distSpecVersion": "1.1.1",
    "storage": {
        "rootDirectory": "${zot_root_dir}",
        "dedupe": false,
        "remoteCache": true,
        "gc": true,
        "gcDelay": "5s",
        "gcInterval": "2s",
        "maxRepos": 2,
        "storageDriver": {
            "name": "s3",
            "rootdirectory": "/zot",
            "region": "us-east-2",
            "regionendpoint": "localhost:4566",
            "bucket": "zot-storage-test",
            "secure": false,
            "skipverify": false
        },
        "cacheDriver": {
            "name": "redis",
            "url": "${redis_url}"
        }
    },
    "http": {
        "address": "${zot_server_address}",
        "port": "${zot_server_port}"
    },
    "log": {
        "level": "debug",
        "output": "${zot_log_file}"
    }
}
EOF
}

function launch_zot_server() {
    local zot_server_address=${1}
    local zot_server_port=${2}
    local redis_url=${3}

    mkdir -p ${ZOT_LOG_DIR}

    local zot_config_file="${BATS_FILE_TMPDIR}/zot_config_${zot_server_address}_${zot_server_port}.json"
    local zot_log_file="${ZOT_LOG_DIR}/zot-${zot_server_address}-${zot_server_port}.log"

    create_locking_config_file ${zot_server_address} ${zot_server_port} ${ZOT_ROOT_DIR} \
        ${zot_config_file} ${zot_log_file} ${redis_url}

    echo "launching zot server ${zot_server_address}:${zot_server_port}" >&3
    zot_serve ${zot_config_file}
    wait_zot_reachable ${zot_server_port}
}

function setup_file() {
    if ! verify_prerequisites; then
        exit 1
    fi

    # one shared redis for all instances
    redis_port=$(get_free_port_for_service "redis")
    redis_start redis_server ${redis_port}
    local redis_url="redis://127.0.0.1:${redis_port}"

    # one shared S3 bucket (localstack) and the dynamodb lock table
    setup_cloud_services

    zot_srv_ports=()
    for ((i=0;i<${NUM_ZOT_INSTANCES};i++)); do
        port=$(get_free_port_for_service "zot${i}")
        zot_srv_ports+=("${port}")
    done

    for inst in "${zot_srv_ports[@]}"; do
        launch_zot_server 127.0.0.1 ${inst} ${redis_url}
    done

    haproxy_port=$(get_free_port_for_service "haproxy")
    echo ${haproxy_port} > ${BATS_FILE_TMPDIR}/haproxy.port

    generate_haproxy_config ${HAPROXY_CFG_FILE} "http" ${haproxy_port} "${zot_srv_ports[@]}"
    haproxy_start ${HAPROXY_CFG_FILE}

    # a small image for skopeo-driven pushes
    skopeo --insecure-policy copy --format=oci docker://ghcr.io/project-zot/golang:1.20 \
        oci:${BATS_FILE_TMPDIR}/golang:1.20 >&3
}

function teardown_file() {
    haproxy_stop_all
    zot_stop_all
    redis_stop redis_server
    teardown_cloud_services
}

function lb_url() {
    echo "http://127.0.0.1:$(cat ${BATS_FILE_TMPDIR}/haproxy.port)"
}

# uploads one shared blob into the repo and prints the manifest body referencing it
function push_shared_blob() {
    local repo=${1}
    local base=$(lb_url)

    local content='{"locked":true}'
    local digest="sha256:$(printf '%s' "${content}" | shasum -a 256 | awk '{print $1}')"

    local location=$(curl -s -D - -o /dev/null -X POST "${base}/v2/${repo}/blobs/uploads/" \
        | tr -d '\r' | awk 'tolower($1) == "location:" {print $2}')
    [ -n "${location}" ]

    case "${location}" in
        http*) ;;
        *) location="${base}${location}" ;;
    esac

    # the upload URL may or may not already carry a query string
    local sep="?"
    case "${location}" in
        *\?*) sep="&" ;;
    esac

    local code=$(curl -s -o /dev/null -w "%{http_code}" -X PUT \
        -H "Content-Type: application/octet-stream" \
        --data-binary "${content}" \
        "${location}${sep}digest=${digest}")
    [ "${code}" -eq 201 ]

    cat <<EOF
{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"${digest}","size":${#content}},"layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","digest":"${digest}","size":${#content}}]}
EOF
}

@test "24 tags of one manifest pushed concurrently through the LB all survive" {
    local base=$(lb_url)
    local manifest=$(push_shared_blob "hammer")
    [ -n "${manifest}" ]

    local pids=()
    for i in $(seq 0 23); do
        local tag=$(printf "t%02d" ${i})
        curl -s -o /dev/null -w "%{http_code}" -X PUT \
            -H "Content-Type: application/vnd.oci.image.manifest.v1+json" \
            -d "${manifest}" \
            "${base}/v2/hammer/manifests/${tag}" > ${BATS_TEST_TMPDIR}/hammer-${tag}.code &
        pids+=($!)
    done

    for p in "${pids[@]}"; do
        wait ${p}
    done

    # every push reported success
    for i in $(seq 0 23); do
        local tag=$(printf "t%02d" ${i})
        [ "$(cat ${BATS_TEST_TMPDIR}/hammer-${tag}.code)" -eq 201 ]
    done

    # every tag is listed and resolves
    run curl -s "${base}/v2/hammer/tags/list"
    [ "$status" -eq 0 ]
    [ $(echo "${lines[-1]}" | jq '.tags | length') -eq 24 ]

    for i in $(seq 0 23); do
        local tag=$(printf "t%02d" ${i})
        local code=$(curl -s -o /dev/null -w "%{http_code}" \
            -H "Accept: application/vnd.oci.image.manifest.v1+json" \
            "${base}/v2/hammer/manifests/${tag}")
        [ "${code}" -eq 200 ]
    done
}

@test "the same tag pushed concurrently from every instance stays consistent" {
    local base=$(lb_url)
    local manifest=$(push_shared_blob "hammer")

    local pids=()
    for i in $(seq 1 12); do
        curl -s -o /dev/null -w "%{http_code}" -X PUT \
            -H "Content-Type: application/vnd.oci.image.manifest.v1+json" \
            -d "${manifest}" \
            "${base}/v2/hammer/manifests/shared" > ${BATS_TEST_TMPDIR}/shared-${i}.code &
        pids+=($!)
    done

    for p in "${pids[@]}"; do
        wait ${p}
    done

    for i in $(seq 1 12); do
        [ "$(cat ${BATS_TEST_TMPDIR}/shared-${i}.code)" -eq 201 ]
    done

    # the tag resolves, and to the manifest every pusher sent
    local want=$(printf '%s' "${manifest}" | shasum -a 256 | awk '{print $1}')
    run curl -s -D - -o /dev/null \
        -H "Accept: application/vnd.oci.image.manifest.v1+json" \
        "${base}/v2/hammer/manifests/shared"
    [ "$status" -eq 0 ]
    echo "${lines[@]}" | tr -d '\r' | grep -i "docker-content-digest" | grep -q "${want}"
}

@test "delete and re-push cycles through the LB never corrupt the repo" {
    local base=$(lb_url)

    for i in $(seq 1 10); do
        run skopeo --insecure-policy copy --dest-tls-verify=false \
            oci:${BATS_FILE_TMPDIR}/golang:1.20 \
            docker://127.0.0.1:$(cat ${BATS_FILE_TMPDIR}/haproxy.port)/race:v${i}
        echo "cycle ${i} skopeo status=${status}: ${lines[@]}" >&3
        [ "$status" -eq 0 ]

        local digest=$(curl -s -o /dev/null -D - \
            -H "Accept: application/vnd.oci.image.manifest.v1+json" \
            "${base}/v2/race/manifests/v${i}" \
            | tr -d '\r' | awk 'tolower($1) == "docker-content-digest:" {print $2}')
        [ -n "${digest}" ]

        local code=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE \
            "${base}/v2/race/manifests/${digest}")
        [ "${code}" -eq 202 ]
    done

    # the last delete emptied the repo, so it must be gone from the catalog
    run curl -s "${base}/v2/_catalog"
    [ "$status" -eq 0 ]
    [ $(echo "${lines[-1]}" | jq 'any(.repositories[]; . == "race") | not') = "true" ]

    # and a fresh push to the same name round-trips intact
    run skopeo --insecure-policy copy --dest-tls-verify=false \
        oci:${BATS_FILE_TMPDIR}/golang:1.20 \
        docker://127.0.0.1:$(cat ${BATS_FILE_TMPDIR}/haproxy.port)/race:final
    [ "$status" -eq 0 ]

    local code=$(curl -s -o /dev/null -w "%{http_code}" \
        -H "Accept: application/vnd.oci.image.manifest.v1+json" \
        "${base}/v2/race/manifests/final")
    [ "${code}" -eq 200 ]
}

@test "quota is enforced across instances and released on delete" {
    local base=$(lb_url)

    # hammer and race hold the two slots
    run curl -s "${base}/v2/_catalog"
    [ "$status" -eq 0 ]
    [ $(echo "${lines[-1]}" | jq '.repositories | length') -eq 2 ]

    # a third repo is refused, whichever instance answers
    local manifest=$(push_shared_blob "third")
    local code=$(curl -s -o /dev/null -w "%{http_code}" -X PUT \
        -H "Content-Type: application/vnd.oci.image.manifest.v1+json" \
        -d "${manifest}" \
        "${base}/v2/third/manifests/v1")
    [ "${code}" -eq 429 ]

    # emptying race through the LB frees its slot
    local digest=$(curl -s -o /dev/null -D - \
        -H "Accept: application/vnd.oci.image.manifest.v1+json" \
        "${base}/v2/race/manifests/final" \
        | tr -d '\r' | awk 'tolower($1) == "docker-content-digest:" {print $2}')
    [ -n "${digest}" ]

    code=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE \
        "${base}/v2/race/manifests/${digest}")
    [ "${code}" -eq 202 ]

    # the freed slot lets a differently named repo in, and the catalog reflects it
    run skopeo --insecure-policy copy --dest-tls-verify=false \
        oci:${BATS_FILE_TMPDIR}/golang:1.20 \
        docker://127.0.0.1:$(cat ${BATS_FILE_TMPDIR}/haproxy.port)/third:v1
    [ "$status" -eq 0 ]

    run curl -s "${base}/v2/_catalog"
    [ "$status" -eq 0 ]
    [ $(echo "${lines[-1]}" | jq '.repositories | sort | join(",")') = '"hammer,third"' ]

    # and the quota bites again once the slot is taken
    manifest=$(push_shared_blob "fourth")
    code=$(curl -s -o /dev/null -w "%{http_code}" -X PUT \
        -H "Content-Type: application/vnd.oci.image.manifest.v1+json" \
        -d "${manifest}" \
        "${base}/v2/fourth/manifests/v1")
    [ "${code}" -eq 429 ]
}
