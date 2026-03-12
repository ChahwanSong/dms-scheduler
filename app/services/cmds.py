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
    "elif [ -L {target_path} ]; then "
    "  echo __SYMLINK__; "
    "elif [ -S {target_path} ]; then "
    "  echo __SOCKET__; "
    "elif [ -p {target_path} ]; then "
    "  echo __FIFO__; "
    "elif [ -b {target_path} ]; then "
    "  echo __BLOCK_DEVICE__; "
    "elif [ -c {target_path} ]; then "
    "  echo __CHAR_DEVICE__; "
    "else "
    "  echo __OTHER__; "
    "fi"
)

# source path 디렉토리가 읽기/실행 가능한지
SYNC_OWNERSHIP_VERIFY_SRC_FILE_CMD = (
    "sudo -u {user_id} bash -c '"
    'if [ -r "$1" ]; then '
    '  echo "__TRUE__"; '
    "else "
    '  echo "__FALSE__"; '
    "fi' bash {target_path} 2>/dev/null"
)
# source path 디렉토리가 읽기/실행 가능한지
SYNC_OWNERSHIP_VERIFY_SRC_DIR_CMD = (
    "sudo -u {user_id} bash -c '"
    'if [ -d "$1" ] && [ -r "$1" ] && [ -x "$1" ]; then '
    '  echo "__TRUE__"; '
    "else "
    '  echo "__FALSE__"; '
    "fi' bash {target_path} 2>/dev/null"
)
# destination path 디렉토리가 쓰기/실행 가능한지
SYNC_OWNERSHIP_VERIFY_DST_CMD = (
    "sudo -u {user_id} bash -c '"
    'if [ -d "$1" ] && [ -w "$1" ] && [ -x "$1" ]; then '
    '  echo "__TRUE__"; '
    "else "
    '  echo "__FALSE__"; '
    "fi' bash {target_path} 2>/dev/null"
)

# target path 디렉토리가 읽기/실행 가능한지 (-r, -x)
HOTCOLD_OWNERSHIP_VERIFY_CMD = (
    "sudo -u {user_id} bash -c '"
    'if [ -r "$1" ]; then '
    '  echo "__TRUE__"; '
    "else "
    '  echo "__FALSE__"; '
    "fi' bash {target_path} 2>/dev/null"
)

# Linux에서 파일 삭제 조건: parent directory에 write 권한 (-w), execute 권한 (-x)
RM_OWNERSHIP_VERIFY_DST_CMD = (
    "sudo -u {user_id} bash -c '"
    'target="$1"; '
    'parent=$(dirname -- "$target"); '
    'if [ ! -w "$parent" ] || [ ! -x "$parent" ]; then '
    '  echo "__FALSE__"; exit; '
    "fi; "
    'if [ -k "$parent" ]; then '
    '  if [ ! -O "$target" ] && [ ! -O "$parent" ]; then '
    '    echo "__FALSE__"; exit; '
    "  fi; "
    "fi; "
    'echo "__TRUE__"; '
    '\' bash "{target_path}" 2>/dev/null'
)

# dsync 템플릿
DSYNC_RUN_CMD = (
    # host:slots,host:slots,... 포맷으로 MPI_HOST 생성
    "MPI_HOST=$(awk -v slots={n_slots_per_host} '{{printf \"%s:%d,\",$0,slots}}' {worker_hostfile} | sed 's/,$//')\n"
    # 전체 호스트 수와 총 프로세스 수 계산
    "NHOSTS=$(grep -cve '^\\s*$' {worker_hostfile})\n"
    "NP=$((NHOSTS * {n_slots_per_host}))\n"
    # dsync 실행
    "$OMPI/bin/mpirun -np $NP "
    "--host $MPI_HOST "
    "--prefix $OMPI "
    "-x LD_LIBRARY_PATH=$OMPI/lib:$LD_LIBRARY_PATH "
    "--mca orte_keep_fqdn_hostnames 1 "
    "--mca plm_rsh_agent ssh "
    ### TCP 버전
    "--mca btl tcp,self "
    "--mca btl_tcp_if_exclude lo "
    "--mca btl_tcp_nodelay 1 "
    "--mca pml ob1 "  # 안정적인 MPI PML 환경 (UCX 는 가끔 PML component 못찾아서 오류남)
    # ### RDMA 버전
    # "--mca btl ^vader,openib,tcp "
    # "--mca pml ucx "
    # "--mca osc ucx "
    # "-x UCX_TLS=rc_x,sm,self "
    # 경로 및 옵션
    "$BINARY_PATH_DSYNC "
    "{options} "
    "{src_path} {dst_path} 2>&1 | tee -a /proc/1/fd/1; "
    "rc=${{PIPESTATUS[0]}}; "
    'if [ "$rc" -eq 0 ]; then '
    "  echo '[DSYNC_STATUS] SUCCESS' | tee -a /proc/1/fd/1; "
    "else "
    '  echo "[DSYNC_STATUS] FAILED (code=$rc)" | tee -a /proc/1/fd/1; '
    "fi; "
    "exit $rc"
)


NSYNC_RUN_CMD = (
    # host:slots,host:slots,... 포맷으로 MPI_HOST 생성
    "MPI_HOST=$(awk -v slots={n_slots_per_host} '{{printf \"%s:%d,\",$0,slots}}' {worker_hostfile} | sed 's/,$//')\n"
    # 전체 호스트 수와 총 프로세스 수 계산
    "NHOSTS=$(grep -cve '^\\s*$' {worker_hostfile})\n"
    "NP=$((NHOSTS * {n_slots_per_host}))\n"
    # nsync 실행
    "$OMPI/bin/mpirun -np $NP "
    "--host $MPI_HOST "
    "--prefix $OMPI "
    "-x LD_LIBRARY_PATH=$OMPI/lib:$LD_LIBRARY_PATH "
    "--mca orte_keep_fqdn_hostnames 1 "
    "--mca plm_rsh_agent ssh "
    "--mca btl tcp,self "
    "--mca btl_tcp_if_exclude lo "
    "--mca btl_tcp_nodelay 1 "
    "--mca pml ob1 "
    "nsync "
    "{options} "
    "{src_path} {dst_path} 2>&1 | tee -a /proc/1/fd/1; "
    "rc=${{PIPESTATUS[0]}}; "
    'if [ "$rc" -eq 0 ]; then '
    "  echo '[NSYNC_STATUS] SUCCESS' | tee -a /proc/1/fd/1; "
    "else "
    '  echo "[NSYNC_STATUS] FAILED (code=$rc)" | tee -a /proc/1/fd/1; '
    "fi; "
    "exit $rc"
)

DRM_RUN_CMD = (
    # host:slots,host:slots,... 포맷으로 MPI_HOST 생성
    "MPI_HOST=$(awk -v slots={n_slots_per_host} '{{printf \"%s:%d,\",$0,slots}}' {worker_hostfile} | sed 's/,$//')\n"
    # 전체 호스트 수와 총 프로세스 수 계산
    "NHOSTS=$(grep -cve '^\\s*$' {worker_hostfile})\n"
    "NP=$((NHOSTS * {n_slots_per_host}))\n"
    # drm 실행
    "$OMPI/bin/mpirun -np $NP "
    "--host $MPI_HOST "
    "--prefix $OMPI "
    "-x LD_LIBRARY_PATH=$OMPI/lib:$LD_LIBRARY_PATH "
    "--mca orte_keep_fqdn_hostnames 1 "
    "--mca plm_rsh_agent ssh "
    ### TCP 버전
    "--mca btl tcp,self "
    "--mca btl_tcp_if_exclude lo "
    "--mca btl_tcp_nodelay 1 "
    "--mca pml ob1 "  # 안정적인 MPI PML 환경 (UCX 는 가끔 PML component 못찾아서 오류남)
    # ### RDMA 버전
    # "--mca btl ^vader,openib,tcp "
    # "--mca pml ucx "
    # "--mca osc ucx "
    # "-x UCX_TLS=rc_x,sm,self "
    # 경로 및 옵션
    "$BINARY_PATH_DRM "
    "{options} "
    "{target_path} 2>&1 | tee -a /proc/1/fd/1; "
    "rc=${{PIPESTATUS[0]}}; "
    'if [ "$rc" -eq 0 ]; then '
    "  echo '[DRM_STATUS] SUCCESS' | tee -a /proc/1/fd/1; "
    "else "
    '  echo "[DRM_STATUS] FAILED (code=$rc)" | tee -a /proc/1/fd/1; '
    "fi; "
    "exit $rc"
)

HOTCOLD_RUN_CMD = (
    # host:slots,host:slots,... 포맷으로 MPI_HOST 생성
    "MPI_HOST=$(awk -v slots={n_slots_per_host} '{{printf \"%s:%d,\",$0,slots}}' {worker_hostfile} | sed 's/,$//')\n"
    # 전체 호스트 수와 총 프로세스 수 계산
    "NHOSTS=$(grep -cve '^\\s*$' {worker_hostfile})\n"
    "NP=$((NHOSTS * {n_slots_per_host}))\n"
    # hotcold 실행 (전용 바이너리가 없으면 drm 바이너리 fallback)
    "$OMPI/bin/mpirun -np $NP "
    "--host $MPI_HOST "
    "--prefix $OMPI "
    "-x LD_LIBRARY_PATH=$OMPI/lib:$LD_LIBRARY_PATH "
    "--mca orte_keep_fqdn_hostnames 1 "
    "--mca plm_rsh_agent ssh "
    "--mca btl tcp,self "
    "--mca btl_tcp_if_exclude lo "
    "--mca btl_tcp_nodelay 1 "
    "--mca pml ob1 "  # 안정적인 MPI PML 환경 (UCX 는 가끔 PML component 못찾아서 오류남)
    # ### RDMA 버전
    # "--mca btl ^vader,openib,tcp "
    # "--mca pml ucx "
    # "--mca osc ucx "
    # "-x UCX_TLS=rc_x,sm,self "
    "$BINARY_PATH_DHOTCOLD "
    "{options} "
    "{target_path} 2>&1 | tee -a /proc/1/fd/1; "
    "rc=${{PIPESTATUS[0]}}; "
    'if [ "$rc" -eq 0 ]; then '
    "  echo '[HOTCOLD_STATUS] SUCCESS' | tee -a /proc/1/fd/1; "
    "else "
    '  echo "[HOTCOLD_STATUS] FAILED (code=$rc)" | tee -a /proc/1/fd/1; '
    "fi; "
    "exit $rc"
)
