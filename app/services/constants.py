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

MOUNT_VERIFY_CMD = (
    "if command -v findmnt >/dev/null 2>&1; then "
    "  findmnt -no FSTYPE,SOURCE,TARGET {mount_point} >/dev/null 2>&1 && echo __OK__ || echo __NOT_FOUND__; "
    "else "
    "  df -T {mount_point} >/dev/null 2>&1 && echo __OK__ || echo __NOT_FOUND__; "
    "fi"
)

K8S_DMS_NAMESPACE = "dms-service"
K8S_VERIFIER_JOB_NAME_PREFIX="vcjob-dir-verifier"
K8S_VERIFIER_JOB_LABEL = "dir-verifier-job-id"

K8S_SYNC_JOB_NAME_PREFIX = "vcjob-dsync"
K8S_SYNC_JOB_LABEL = "sync-job-id"

SYNC_VERIFIER_TEMPLATE = "/dms/kube-dms-backend/template/dms-vcjob-dir-verifier.yaml"
SYNC_JOB_TEMPLATE = "/dms/kube-dms-backend/template/dms-vcjob-dsync.yaml"
