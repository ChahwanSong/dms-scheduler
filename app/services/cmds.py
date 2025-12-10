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

# dsync 템플릿
DSYNC_RUN_CMD = (
    # host:slots,host:slots,... 포맷으로 MPI_HOST 생성
    "MPI_HOST=$(awk -v slots={n_slots_per_host} '{{printf \"%s:%d,\",$0,slots}}' {worker_hostfile} | sed 's/,$//')\n"
    # 전체 호스트 수와 총 프로세스 수 계산
    "NHOSTS=$(grep -cve '^\\s*$' {worker_hostfile})\n"
    "NP=$((NHOSTS * {n_slots_per_host}))\n"
    
    # dsync 실행 
    "/usr/local/openmpi-4.1.8/bin/mpirun -np $NP "
    "--host $MPI_HOST "
    "--prefix $OMPI "
    "-x LD_LIBRARY_PATH=$OMPI/lib:$LD_LIBRARY_PATH "
    "--mca orte_keep_fqdn_hostnames 1 "
    "--mca plm_rsh_agent ssh "
    
    # ### TCP 버전 
    # "--mca btl tcp,self "
    # "--mca btl_tcp_if_exclude lo "
    # "--mca pml ob1 "
    # "-x UCX_TLS=tcp,sm,self "
    
    ### RDMA 버전
    "--mca btl ^vader,openib,tcp "
    "--mca pml ucx "
    "--mca osc ucx "
    "-x UCX_TLS=rc_x,sm,self "

    # 경로
    "$BINARY_PATH_DSYNC "
    "{options} "
    "{src_path} {dst_path} 2>&1 | tee -a /proc/1/fd/1; exit ${{PIPESTATUS[0]}}"
)
