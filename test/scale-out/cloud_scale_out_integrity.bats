# Data-integrity and HA tests for multi-instance deployments over shared storage.
# Three instances, one S3 bucket, one shared redis, no cluster block, round-robin LB,
# GC sweeping every 2s. Where the locking suite attacks races, this suite verifies the
# state afterwards: blob content verifies by digest, deletes settle consistently, a dead
# instance loses nothing acknowledged, GC collects only the dead, restarts heal, and the
# catalog, the storage layout and the metadata store agree.

NUM_ZOT_INSTANCES=3
ZOT_LOG_DIR=/tmp/zot-ft-logs/integrity

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

function create_integrity_config_file() {
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
        "gcDelay": "30s",
        "gcInterval": "2s",
        "maxRepos": 100,
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

    create_integrity_config_file ${zot_server_address} ${zot_server_port} ${ZOT_ROOT_DIR} \
        ${zot_config_file} ${zot_log_file} ${redis_url}

    echo "launching zot server ${zot_server_address}:${zot_server_port}" >&3
    zot_serve ${zot_config_file}
    wait_zot_reachable ${zot_server_port}
}

function setup_file() {
    if ! verify_prerequisites; then
        exit 1
    fi

    redis_port=$(get_free_port_for_service "redis")
    redis_start redis_server ${redis_port}
    echo ${redis_port} > ${BATS_FILE_TMPDIR}/redis.port
    local redis_url="redis://127.0.0.1:${redis_port}"

    setup_cloud_services

    zot_srv_ports=()
    for ((i=0;i<${NUM_ZOT_INSTANCES};i++)); do
        port=$(get_free_port_for_service "zot${i}")
        zot_srv_ports+=("${port}")
    done
    echo "${zot_srv_ports[@]}" > ${BATS_FILE_TMPDIR}/zot.ports

    for inst in "${zot_srv_ports[@]}"; do
        launch_zot_server 127.0.0.1 ${inst} ${redis_url}
    done

    haproxy_port=$(get_free_port_for_service "haproxy")
    echo ${haproxy_port} > ${BATS_FILE_TMPDIR}/haproxy.port

    generate_haproxy_config ${HAPROXY_CFG_FILE} "http" ${haproxy_port} "${zot_srv_ports[@]}"
    # production LBs health-check: mark a dead backend out instead of routing to it
    sed -i '' 's|^\(    server zot[0-9] 127.0.0.1:[0-9]*\)$|\1 check inter 1s fall 1 rise 1|' ${HAPROXY_CFG_FILE}
    haproxy_start ${HAPROXY_CFG_FILE}

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

# verify_repo_content fetches every tag's manifest and every referenced blob through the
# LB and checks each blob's sha256 against its digest.
function verify_repo_content() {
    local repo=${1}
    local base=$(lb_url)

    local tags=$(curl -s "${base}/v2/${repo}/tags/list" | jq -r '.tags[]')
    [ -n "${tags}" ]

    for tag in ${tags}; do
        local manifest=$(curl -s -H "Accept: application/vnd.oci.image.manifest.v1+json" \
            "${base}/v2/${repo}/manifests/${tag}")
        [ -n "${manifest}" ]

        local digests=$(echo "${manifest}" | jq -r '.config.digest, .layers[].digest')
        [ -n "${digests}" ]

        for dgst in ${digests}; do
            local got=$(curl -s "${base}/v2/${repo}/blobs/${dgst}" | shasum -a 256 | awk '{print $1}')
            local want=$(echo "${dgst}" | cut -d: -f2)
            if [ "${got}" != "${want}" ]; then
                echo "blob ${dgst} of ${repo}:${tag} corrupted: got sha256:${got}" >&3
                return 1
            fi
        done
    done
}

@test "every pushed tag's blobs verify by digest through every instance" {
    local base=$(lb_url)
    local haproxy_port=$(cat ${BATS_FILE_TMPDIR}/haproxy.port)

    # one real multi-blob image, then eleven more tags of it pushed concurrently
    run skopeo --insecure-policy copy --dest-tls-verify=false \
        oci:${BATS_FILE_TMPDIR}/golang:1.20 \
        docker://127.0.0.1:${haproxy_port}/integrity:t01
    [ "$status" -eq 0 ]

    local manifest=$(curl -s -H "Accept: application/vnd.oci.image.manifest.v1+json" \
        "${base}/v2/integrity/manifests/t01")
    [ -n "${manifest}" ]

    local pids=()
    for i in $(seq 2 12); do
        local tag=$(printf "t%02d" ${i})
        curl -s -o /dev/null -w "%{http_code}" -X PUT \
            -H "Content-Type: application/vnd.oci.image.manifest.v1+json" \
            -d "${manifest}" \
            "${base}/v2/integrity/manifests/${tag}" > ${BATS_TEST_TMPDIR}/it-${tag}.code &
        pids+=($!)
    done

    for p in "${pids[@]}"; do
        wait ${p}
    done

    for i in $(seq 2 12); do
        local tag=$(printf "t%02d" ${i})
        [ "$(cat ${BATS_TEST_TMPDIR}/it-${tag}.code)" -eq 201 ]
    done

    # every tag, every config and layer blob, verified by content hash
    verify_repo_content "integrity"
}

@test "100 then 200 tags of one manifest concurrently through the LB all survive and verify" {
    local base=$(lb_url)

    # one shared blob for the repo
    local content='{"scale":true}'
    local digest="sha256:$(printf '%s' "${content}" | shasum -a 256 | awk '{print $1}')"
    local location=$(curl -s -D - -o /dev/null -X POST "${base}/v2/big/blobs/uploads/" \
        | tr -d '\r' | awk 'tolower($1) == "location:" {print $2}')
    case "${location}" in
        http*) ;;
        *) location="${base}${location}" ;;
    esac
    local sep="?"
    case "${location}" in
        *\?*) sep="&" ;;
    esac
    [ $(curl -s -o /dev/null -w "%{http_code}" -X PUT \
        -H "Content-Type: application/octet-stream" \
        --data-binary "${content}" "${location}${sep}digest=${digest}") -eq 201 ]

    local manifest="{\"schemaVersion\":2,\"mediaType\":\"application/vnd.oci.image.manifest.v1+json\",\"config\":{\"mediaType\":\"application/vnd.oci.image.config.v1+json\",\"digest\":\"${digest}\",\"size\":${#content}},\"layers\":[{\"mediaType\":\"application/vnd.oci.image.layer.v1.tar+gzip\",\"digest\":\"${digest}\",\"size\":${#content}}]}"

    hammer() {
        local from=${1}
        local to=${2}

        local pids=()
        for i in $(seq ${from} ${to}); do
            local tag=$(printf "t%03d" ${i})
            curl -s -o /dev/null -w "%{http_code}" --max-time 120 -X PUT \
                -H "Content-Type: application/vnd.oci.image.manifest.v1+json" \
                -d "${manifest}" \
                "${base}/v2/big/manifests/${tag}" > ${BATS_TEST_TMPDIR}/big-${tag}.code &
            pids+=($!)
        done

        for p in "${pids[@]}"; do
            wait ${p}
        done

        for i in $(seq ${from} ${to}); do
            local tag=$(printf "t%03d" ${i})
            local code=$(cat ${BATS_TEST_TMPDIR}/big-${tag}.code)
            if [ "${code}" -ne 201 ]; then
                echo "push of big:${tag} got ${code}" >&3
                return 1
            fi
        done
    }

    # first hundred concurrent
    hammer 0 99
    [ $(curl -s "${base}/v2/big/tags/list" | jq '.tags | length') -eq 100 ]

    # second hundred concurrent, over the index the first hundred wrote
    hammer 100 199
    [ $(curl -s "${base}/v2/big/tags/list" | jq '.tags | length') -eq 200 ]

    # every tag resolves and its content verifies
    verify_repo_content "big"
}

@test "concurrent pushes of the same image to many repos all land intact" {
    local haproxy_port=$(cat ${BATS_FILE_TMPDIR}/haproxy.port)

    # ten repos pushed at once through the LB: concurrent repo creation and concurrent
    # uploads of the same blob digests into different repos on shared storage
    local pids=()
    for i in $(seq 1 10); do
        skopeo --insecure-policy copy --dest-tls-verify=false \
            oci:${BATS_FILE_TMPDIR}/golang:1.20 \
            docker://127.0.0.1:${haproxy_port}/many$(printf "%02d" ${i}):v1 \
            > ${BATS_TEST_TMPDIR}/many-${i}.log 2>&1 &
        pids+=($!)
    done

    for p in "${pids[@]}"; do
        wait ${p}
    done

    for i in $(seq 1 10); do
        if [ -s ${BATS_TEST_TMPDIR}/many-${i}.log ] && grep -q "level=fatal" ${BATS_TEST_TMPDIR}/many-${i}.log; then
            echo "push to many$(printf "%02d" ${i}) failed: $(cat ${BATS_TEST_TMPDIR}/many-${i}.log)" >&3
            return 1
        fi
    done

    # every repo's content verifies by digest
    for i in $(seq 1 10); do
        verify_repo_content "many$(printf "%02d" ${i})"
    done
}

@test "concurrent pushes of the same image to one repo interleave blob uploads safely" {
    local haproxy_port=$(cat ${BATS_FILE_TMPDIR}/haproxy.port)

    # six clients push the same image to the same repo at once under different tags:
    # the blob uploads of identical digests interleave on shared storage
    local pids=()
    for i in $(seq 1 6); do
        skopeo --insecure-policy copy --dest-tls-verify=false \
            oci:${BATS_FILE_TMPDIR}/golang:1.20 \
            docker://127.0.0.1:${haproxy_port}/sharedrepo:sk${i} \
            > ${BATS_TEST_TMPDIR}/sk-${i}.log 2>&1 &
        pids+=($!)
    done

    for p in "${pids[@]}"; do
        wait ${p}
    done

    for i in $(seq 1 6); do
        if grep -q "level=fatal" ${BATS_TEST_TMPDIR}/sk-${i}.log 2>/dev/null; then
            echo "push sk${i} failed: $(cat ${BATS_TEST_TMPDIR}/sk-${i}.log)" >&3
            return 1
        fi
    done

    local base=$(lb_url)
    [ $(curl -s "${base}/v2/sharedrepo/tags/list" | jq '.tags | length') -eq 6 ]

    verify_repo_content "sharedrepo"
}

@test "concurrent deletes of the same manifest settle consistently" {
    local base=$(lb_url)
    local haproxy_port=$(cat ${BATS_FILE_TMPDIR}/haproxy.port)

    run skopeo --insecure-policy copy --dest-tls-verify=false \
        oci:${BATS_FILE_TMPDIR}/golang:1.20 \
        docker://127.0.0.1:${haproxy_port}/victim:v1
    [ "$status" -eq 0 ]

    local digest=$(curl -s -o /dev/null -D - \
        -H "Accept: application/vnd.oci.image.manifest.v1+json" \
        "${base}/v2/victim/manifests/v1" \
        | tr -d '\r' | awk 'tolower($1) == "docker-content-digest:" {print $2}')
    [ -n "${digest}" ]

    # eight concurrent deletes of the same digest through the LB
    local pids=()
    for i in $(seq 1 8); do
        curl -s -o /dev/null -w "%{http_code}" -X DELETE \
            "${base}/v2/victim/manifests/${digest}" > ${BATS_TEST_TMPDIR}/del-${i}.code &
        pids+=($!)
    done

    for p in "${pids[@]}"; do
        wait ${p}
    done

    # every answer is a legitimate one: accepted, or already gone. Never a server error.
    local accepted=0
    for i in $(seq 1 8); do
        local code=$(cat ${BATS_TEST_TMPDIR}/del-${i}.code)
        [ "${code}" -ne 500 ]
        if [ "${code}" -eq 202 ]; then
            accepted=$((accepted+1))
        fi
    done
    [ "${accepted}" -ge 1 ]

    # the tag is gone, whichever instance answers
    for i in $(seq 1 6); do
        local code=$(curl -s -o /dev/null -w "%{http_code}" \
            -H "Accept: application/vnd.oci.image.manifest.v1+json" \
            "${base}/v2/victim/manifests/v1")
        [ "${code}" -eq 404 ]
    done
}

@test "an instance dying mid-hammer loses nothing acknowledged and rejoins" {
    local base=$(lb_url)
    local haproxy_port=$(cat ${BATS_FILE_TMPDIR}/haproxy.port)
    local ports=($(cat ${BATS_FILE_TMPDIR}/zot.ports))
    local victim_port=${ports[0]}

    # a blob to push manifests over
    local content='{"ha":true}'
    local digest="sha256:$(printf '%s' "${content}" | shasum -a 256 | awk '{print $1}')"
    local location=$(curl -s -D - -o /dev/null -X POST "${base}/v2/survivor/blobs/uploads/" \
        | tr -d '\r' | awk 'tolower($1) == "location:" {print $2}')
    case "${location}" in
        http*) ;;
        *) location="${base}${location}" ;;
    esac
    local sep="?"
    case "${location}" in
        *\?*) sep="&" ;;
    esac
    [ $(curl -s -o /dev/null -w "%{http_code}" -X PUT \
        -H "Content-Type: application/octet-stream" \
        --data-binary "${content}" "${location}${sep}digest=${digest}") -eq 201 ]

    local manifest="{\"schemaVersion\":2,\"mediaType\":\"application/vnd.oci.image.manifest.v1+json\",\"config\":{\"mediaType\":\"application/vnd.oci.image.config.v1+json\",\"digest\":\"${digest}\",\"size\":${#content}},\"layers\":[{\"mediaType\":\"application/vnd.oci.image.layer.v1.tar+gzip\",\"digest\":\"${digest}\",\"size\":${#content}}]}"

    # start the hammer, then kill one instance while pushes are in flight
    local pids=()
    for i in $(seq 0 23); do
        local tag=$(printf "t%02d" ${i})
        curl -s -o /dev/null -w "%{http_code}" -X PUT \
            -H "Content-Type: application/vnd.oci.image.manifest.v1+json" \
            -d "${manifest}" \
            "${base}/v2/survivor/manifests/${tag}" > ${BATS_TEST_TMPDIR}/ha-${tag}.code &
        pids+=($!)
    done

    sleep 1
    local victim_pid=$(pgrep -f "zot_config_127.0.0.1_${victim_port}.json")
    [ -n "${victim_pid}" ]
    kill ${victim_pid}

    for p in "${pids[@]}"; do
        wait ${p}
    done

    # wait for the graceful shutdown to fully release the port before restarting
    for i in $(seq 1 50); do
        kill -0 ${victim_pid} 2>/dev/null || break
        sleep 0.2
    done

    # restart the dead instance on the same config before verifying anything, so the
    # checks below run against a full fleet
    zot_serve ${BATS_FILE_TMPDIR}/zot_config_127.0.0.1_${victim_port}.json
    wait_zot_reachable ${victim_port}

    # pushes either succeeded or failed loudly; a 201 must always mean the tag exists
    local succeeded=0
    for i in $(seq 0 23); do
        local tag=$(printf "t%02d" ${i})
        local code=$(cat ${BATS_TEST_TMPDIR}/ha-${tag}.code)
        if [ "${code}" -eq 201 ]; then
            succeeded=$((succeeded+1))
            local got=$(curl -s -o /dev/null -w "%{http_code}" \
                -H "Accept: application/vnd.oci.image.manifest.v1+json" \
                "${base}/v2/survivor/manifests/${tag}")
            if [ "${got}" -ne 200 ]; then
                echo "push ${tag} returned 201 but the tag is missing afterwards" >&3
                return 1
            fi
        fi
    done

    # the LB kept serving through the remaining instances
    [ "${succeeded}" -ge 12 ]

    # the restarted instance serves the shared state directly
    local code=$(curl -s -o /dev/null -w "%{http_code}" \
        -H "Accept: application/vnd.oci.image.manifest.v1+json" \
        "http://127.0.0.1:${victim_port}/v2/integrity/manifests/t01")
    [ "${code}" -eq 200 ]
}

@test "GC under concurrency collects only the dead" {
    local base=$(lb_url)

    # victim was deleted above; give the sweeps time to reap its orphaned content
    sleep 10

    # the live repos still verify completely: no over-collection
    verify_repo_content "integrity"
    verify_repo_content "survivor"

    # the deleted repo's content is gone from storage: no orphan objects left behind.
    # (the s3 driver prefixes twice with this config shape: rootdirectory + store root)
    run awslocal s3api list-objects-v2 --bucket zot-storage-test --prefix zot/zot/victim/ \
        --query 'length(Contents || `[]`)' --output text
    [ "$status" -eq 0 ]
    [ "${lines[-1]}" -eq 0 ]
}

@test "all instances restart over the same storage and serve the same state" {
    local base=$(lb_url)
    local ports=($(cat ${BATS_FILE_TMPDIR}/zot.ports))

    local before=$(curl -s "${base}/v2/_catalog" | jq -S '.repositories')
    [ -n "${before}" ]

    zot_stop_all
    sleep 2

    for cfg in ${BATS_FILE_TMPDIR}/zot_config_*.json; do
        zot_serve ${cfg}
    done

    for inst in "${ports[@]}"; do
        wait_zot_reachable ${inst}
    done

    local after=$(curl -s "${base}/v2/_catalog" | jq -S '.repositories')
    [ "${before}" = "${after}" ]

    # content still verifies after the restart
    verify_repo_content "integrity"

    # and a fresh push works through the restarted fleet
    local haproxy_port=$(cat ${BATS_FILE_TMPDIR}/haproxy.port)
    run skopeo --insecure-policy copy --dest-tls-verify=false \
        oci:${BATS_FILE_TMPDIR}/golang:1.20 \
        docker://127.0.0.1:${haproxy_port}/integrity:after-restart
    [ "$status" -eq 0 ]
}

@test "catalog, storage and metadb agree" {
    local base=$(lb_url)
    local redis_port=$(cat ${BATS_FILE_TMPDIR}/redis.port)

    local catalog=$(curl -s "${base}/v2/_catalog" | jq -S -r '.repositories | sort | join(",")')

    # the s3 driver prefixes twice with this config shape: rootdirectory + store root
    local in_storage=$(awslocal s3api list-objects-v2 --bucket zot-storage-test \
        --prefix zot/zot/ --delimiter / --output json \
        | jq -r '[.CommonPrefixes[].Prefix | sub("^zot/zot/";"") | sub("/$";"")] | sort | join(",")')

    local in_meta=$(redis-cli -p ${redis_port} HKEYS zot:RepoMeta | sort | tr '\n' ',' | sed 's/,$//')

    echo "catalog:  ${catalog}" >&3
    echo "storage:  ${in_storage}" >&3
    echo "metadb:   ${in_meta}" >&3

    [ "${catalog}" = "${in_storage}" ]
    [ "${catalog}" = "${in_meta}" ]
}
