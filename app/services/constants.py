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

K8S_SYNC_VERIFIER_TEMPLATE = "/dms/kube-dms-backend/template/dms-vcjob-dir-verifier.yaml"
K8S_SYNC_VERIFIER_JOB_NAME_PREFIX="vcjob-dir-verifier"
K8S_SYNC_VERIFIER_JOB_LABEL = "dir-verifier-job-id"
K8S_SYNC_VERIFIER_JOB_IMAGE = "rts2411:5000/dms-verifier:latest"

K8S_SYNC_JOB_NAME_PREFIX = "vcjob-sync"
K8S_SYNC_JOB_LABEL = "sync-job-id"
K8S_SYNC_D_JOB_IMAGE = "rts2411:5000/dms-mfu:latest"
K8S_SYNC_D_JOB_TEMPLATE = "/dms/kube-dms-backend/template/dms-vcjob-sync-d.yaml"
K8S_SYNC_D_WORKER_HOSTFILE_PATH = "/etc/volcano/sync_worker.host"
K8S_SYNC_D_DEFAULT_N_BATCH_FILES = "1000000"
K8S_SYNC_D_DEFAULT_N_SLOTS_PER_HOST = "5"

K8S_VOLCANO_HIGH_PRIO_Q = "high-q"
K8S_VOLCANO_LOW_PRIO_Q = "low-q"


