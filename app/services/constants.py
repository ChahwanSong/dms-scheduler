ALLOWED_SERVICES = {"sync", "hotcold", "rm", "cp", "chmod"}

ALLOWED_DIRECTORIES = {
    "/pvs": 
        {
            "label": "mount-pvs", 
            "info": "IBM GPFS 2024 SSC PVS Storage",
        },
    "/home/gpu1":
        {
            "label": "mount-gpu1",
            "info": "IBM GPFS 2024 SSC Home GPU Storage",
        }
}

K8S_DMS_NAMESPACE = "dms-service"

K8S_VERIFIER_JOB_NAME_PREFIX="vcjob-dir-verifier"
K8S_VERIFIER_JOB_LABEL = "dir-verifier-job-id"
K8S_VERIFIER_JOB_IMAGE = "rts2411:5000/dms-verifier:latest"

K8S_SYNC_JOB_NAME_PREFIX = "vcjob-dsync"
K8S_SYNC_JOB_LABEL = "sync-job-id"

K8S_VOLCANO_HIGH_PRIO_Q = "high-q"
K8S_VOLCANO_LOW_PRIO_Q = "low-q"

K8S_SYNC_VERIFIER_TEMPLATE = "/dms/kube-dms-backend/template/dms-vcjob-dir-verifier.yaml"
K8S_SYNC_JOB_TEMPLATE = "/dms/kube-dms-backend/template/dms-vcjob-dsync.yaml"

# verify whether the storage is mounted on {mount_point} directory
MOUNT_VERIFY_CMD = (
    "if command -v findmnt >/dev/null 2>&1; then "
    "  findmnt -no FSTYPE,SOURCE,TARGET {mount_point} >/dev/null 2>&1 && echo __TRUE__ || echo __FALSE__; "
    "else "
    "  df -T {mount_point} >/dev/null 2>&1 && echo __TRUE__ || echo __FALSE__; "
    "fi"
)

# verify whether the path exists and is a directory
PATHTYPE_VERIFY_CMD = (
    "if [ ! -e {target_path} ]; then "
    "  echo __NOT_FOUND__; "
    "elif [ -f {target_path} ]; then "
    "  echo __FILE__; "
    "elif [ -d {target_path} ]; then "
    "  echo __DIR__; "
    "else "
    "  echo __OTHER__; "
    "fi"
)

# source path 디렉토리가 읽기/실행 가능한지
OWNERSHIP_VERIFY_SRC_FILE_CMD = (
    "sudo -u {user_id} bash -c '"
    "if [ -r \"$1\" ]; then "
    "  echo \"__TRUE__\"; "
    "else "
    "  echo \"__FALSE__\"; "
    "fi' bash {target_path} 2>/dev/null"
)
# source path 디렉토리가 읽기/실행 가능한지
OWNERSHIP_VERIFY_SRC_DIR_CMD = (
    "sudo -u {user_id} bash -c '"
    "if [ -d \"$1\" ] && [ -r \"$1\" ] && [ -x \"$1\" ]; then "
    "  echo \"__TRUE__\"; "
    "else "
    "  echo \"__FALSE__\"; "
    "fi' bash {target_path} 2>/dev/null"
)
# destination path 디렉토리가 쓰기/실행 가능한지
OWNERSHIP_VERIFY_DST_CMD = (
    "sudo -u {user_id} bash -c '"
    "if [ -d \"$1\" ] && [ -w \"$1\" ] && [ -x \"$1\" ]; then "
    "  echo \"__TRUE__\"; "
    "else "
    "  echo \"__FALSE__\"; "
    "fi' bash {target_path} 2>/dev/null"
)
