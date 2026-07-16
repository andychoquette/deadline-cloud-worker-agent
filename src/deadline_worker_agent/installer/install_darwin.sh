#!/usr/bin/env bash

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

#
# AWS Deadline Cloud Worker Agent Installer (macOS / darwin)
#
# This is the macOS port of install.sh. It mirrors the Linux installer's flag surface and
# overall structure so the deadline_worker_agent.installer dispatcher can invoke it identically
# (same getopt arguments, including --python-interpreter-path and --scripts-path).
#
# The installer:
#     1.  Creates a hidden system OS user for the worker agent if required (via Directory Services)
#     2.  Creates an OS group for all job users if required (via Directory Services)
#     3.  Provisions directories used by the worker agent at runtime (identical to Linux).
#     4.  Creates an agent configuration file if required and installs an example configuration file.
#     5.  Updates the configuration file with arguments passed to the installer.
#     6.  Creates, enables, and (optionally) starts a launchd LaunchDaemon that runs the worker
#         agent and restarts it upon failure.
#
# PORTING NOTES (macOS differs from Linux):
#   * Users/groups: dscl/dseditgroup instead of useradd/groupadd/getent/usermod.
#   * Service:      launchd LaunchDaemon instead of a systemd unit.
#   * Shutdown:     /sbin/shutdown -h now instead of /usr/sbin/shutdown now.
#   * VFS:          NOT supported on macOS -- hard error if requested.
#   * Directories, permission modes, worker.toml handling, and the python config call are
#     kept IDENTICAL to the Linux installer (they are portable to macOS/BSD).

set -euo pipefail

SCRIPT_DIR=$(cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# Defaults
default_wa_user=deadline-worker
default_job_group=deadline-job-users
farm_id="unset"
fleet_id="unset"
wa_user=$default_wa_user
confirm=""
region="unset"
scripts_path="unset"
worker_agent_program="deadline-worker-agent"
allow_shutdown="no"
disallow_instance_profile="no"
no_install_service="no"
start_service="no"
telemetry_opt_out="no"
warning_lines=()
vfs_install_path="unset"
python_interpreter_path="unset"
# macOS seals the root volume read-only (macOS 10.15+), so /sessions (a top-level directory)
# cannot be created. Default to a writable path under /var. The dispatcher normally passes
# --session-root-dir explicitly; this is the fallback for a direct script invocation.
session_root_dir="/var/lib/deadline/sessions"

# macOS-specific constants
# NOTE: launchd label + plist path. Uses reverse-DNS label convention that launchd expects.
# UNVERIFIED: ensure no other daemon on the fleet image already uses this label.
launchd_label="com.amazon.deadline.worker-agent"
launchd_plist="/Library/LaunchDaemons/${launchd_label}.plist"
# NOTE: macOS has no `useradd -m`; we must create and own a home directory ourselves.
worker_agent_homedir="/var/lib/deadline-worker"

usage()
{
    echo "Usage: install_darwin.sh --farm-id FARM_ID"
    echo "                  --fleet-id FLEET_ID"
    echo "                  --scripts-path SCRIPTS_PATH"
    echo "                  --python-interpreter-path PYTHON_INTERPRETER_PATH"
    echo "                  --region REGION"
    echo "                  [--user USER]"
    echo "                  [--group GROUP]"
    echo "                  [-y]"
    echo "                  [--disallow-instance-profile]"
    echo "                  [--no-install-service]"
    echo "                  [--allow-shutdown]"
    echo "                  [--session-root-dir SESSION_ROOT_DIR]"
    echo ""
    echo "Arguments"
    echo "---------"
    echo "    --farm-id FARM_ID"
    echo "        The AWS Deadline Cloud Farm ID that the Worker belongs to."
    echo "    --fleet-id FLEET_ID"
    echo "        The AWS Deadline Cloud Fleet ID that the Worker belongs to."
    echo "    --region REGION"
    echo "        The AWS region of the AWS Deadline Cloud farm."
    echo "    --user USER"
    echo "        A user name that the AWS Deadline Cloud Worker Agent will run as. Defaults to $default_wa_user."
    echo "    --group GROUP"
    echo "        A group name that the Worker Agent shares with the user(s) that Jobs will be running as."
    echo "        Do not use the primary/effective group of the Worker Agent user specified in --user as"
    echo "        this is not a secure configuration. Defaults to $default_job_group."
    echo "    --scripts-path SCRIPTS_PATH"
    echo "        An optional path to the directory that the Worker Agent is installed. This is used as the"
    echo "        program path when creating the launchd service for the Worker Agent."
    echo "    --python-interpreter-path"
    echo "        Path to the Python interpreter for the worker agent."
    echo "    --allow-shutdown"
    echo "        Dictates whether a sudoers rule is created/deleted allowing the worker agent the"
    echo "        ability to shutdown the host system."
    echo "    --no-install-service"
    echo "        Skips the worker agent launchd service installation."
    echo "    --telemetry-opt-out"
    echo "        Opts out of telemetry collection for the worker agent."
    echo "    --start"
    echo "        Starts the launchd service as part of the installation."
    echo "    -y"
    echo "        Skips a confirmation prompt before performing the installation."
    echo "    --vfs-install-path VFS_INSTALL_PATH"
    echo "        NOT SUPPORTED on macOS. Providing this option is an error."
    echo "    --disallow-instance-profile"
    echo "        Disallow running the worker agent with an EC2 instance profile."
    echo "    --session-root-dir SESSION_ROOT_DIR"
    echo "        The root directory under which the worker agent will create session directories."

    exit 2
}

banner() {
    echo "==========================================================="
    echo "|      AWS Deadline Cloud Worker Agent Installer       |"
    echo "|                    (macOS / darwin)                     |"
    echo "==========================================================="
}

# --- macOS Directory Services helpers -------------------------------------------------
# macOS has no /etc/passwd-backed `id`/`getent` semantics for lookups the way Linux does;
# we query the local Directory Services node ("." = /Local/Default) with dscl.

user_exists() {
    # `dscl . -read /Users/<name>` exits non-zero if the record does not exist.
    dscl . -read /Users/"$1" &> /dev/null
}

group_exists() {
    dscl . -read /Groups/"$1" &> /dev/null
}

# Primary group NAME of a user (Linux `id -gn` equivalent). Resolves PrimaryGroupID -> group name.
user_primary_group_name() {
    local u="$1" gid
    gid=$(dscl . -read /Users/"$u" PrimaryGroupID 2>/dev/null | awk '{print $2}')
    if [[ -n "${gid}" ]]; then
        dscl . -search /Groups PrimaryGroupID "${gid}" 2>/dev/null | awk 'NR==1{print $1}'
    fi
}

# Allocate the lowest unused ID in [200,500) from a dscl attribute listing.
# NOTE: macOS reserves IDs < 500 for hidden/system accounts. Combined with IsHidden=1 this keeps
#       the agent account out of the login window.
# UNVERIFIED: the free range on the target image; MDM-managed fleets may already occupy low IDs.
#             This allocation is also not race-safe against concurrent account creation.
allocate_system_id() {
    local kind="$1"   # "Users" or "Groups"
    local attr="$2"   # "UniqueID" or "PrimaryGroupID"
    local used candidate
    used=$(dscl . -list /"${kind}" "${attr}" 2>/dev/null | awk '{print $2}' | sort -n)
    for candidate in $(seq 499 -1 200); do
        if ! grep -qx "${candidate}" <<< "${used}"; then
            echo "${candidate}"
            return 0
        fi
    done
    echo "ERROR: Could not allocate a free system ${attr} in range [200,500)." >&2
    return 1
}

validate_deadline_id() {
    prefix="$1"
    input="$2"
    [[ "${input}" =~ ^$prefix-[a-f0-9]{32}$ ]]
}

# Validate arguments
# macOS ships only BSD getopt, which does NOT support `--longoptions` -- it silently treats the
# long option spec as positional args and drops every real flag, leaving values "unset". Rather
# than depend on GNU getopt being on PATH, we parse the long options directly with a portable
# while-loop.

while [[ $# -gt 0 ]]
do
    case "${1}" in
    --farm-id)                      farm_id="$2"                    ; shift 2 ;;
    --fleet-id)                     fleet_id="$2"                   ; shift 2 ;;
    --region)                       region="$2"                     ; shift 2 ;;
    --user)                         wa_user="$2"                    ; shift 2 ;;
    --group)                        job_group="$2"                  ; shift 2 ;;
    --scripts-path)                 scripts_path="$2"               ; shift 2 ;;
    --python-interpreter-path)      python_interpreter_path="$2"    ; shift 2 ;;
    --vfs-install-path)             vfs_install_path="$2"           ; shift 2 ;;
    --session-root-dir)             session_root_dir="$2"           ; shift 2 ;;
    --allow-shutdown)               allow_shutdown="yes"            ; shift   ;;
    --disallow-instance-profile)    disallow_instance_profile="yes" ; shift   ;;
    --no-install-service)           no_install_service="yes"        ; shift   ;;
    --telemetry-opt-out)            telemetry_opt_out="yes"         ; shift   ;;
    --start)                        start_service="yes"             ; shift   ;;
    -y)                             confirm="-y"                    ; shift   ;;
    --) shift; break ;;
    *) echo "ERROR: Unexpected option: $1"
       usage ;;
  esac
done

# Require root (sudo), like the Linux installer. macOS account/plist operations need root.
if [[ "$(id -u)" -ne 0 ]]; then
    echo "ERROR: This installer must be run as root (via sudo)."
    exit 1
fi

# Validate required command-line arguments
if [[ "${farm_id}" == "unset" ]]; then
    echo "ERROR: --farm-id not specified"
    usage
elif ! validate_deadline_id farm "${farm_id}"; then
    echo "ERROR: Not a valid value for --farm-id: ${farm_id}"
    usage
fi

if [[ "${fleet_id}" == "unset" ]]; then
    echo "ERROR: --fleet-id not specified"
    usage
elif ! validate_deadline_id fleet "${fleet_id}"; then
    echo "ERROR: Not a valid value for --fleet-id: ${fleet_id}"
    usage
fi

if [[ "${scripts_path}" == "unset" ]]; then
    echo "ERROR: --scripts-path is not specified"
    usage
elif [[ ! -d "${scripts_path}" ]]; then
    echo "ERROR: The specified scripts path is not found: \"${scripts_path}\""
    usage
else
    set +e
    worker_agent_program="${scripts_path}"/deadline-worker-agent
    if [[ ! -f "${worker_agent_program}" ]]; then
        echo "ERROR: Could not find deadline-worker-agent in scripts path: \"${worker_agent_program}\""
        exit 1
    fi
    set -e
fi

if [[ "${python_interpreter_path}" == "unset" ]]; then
    echo "ERROR: --python-interpreter-path is not specified"
    usage
elif [[ ! -f "${python_interpreter_path}" ]]; then
    echo "ERROR: The Python interpreter path is not found: \"${python_interpreter_path}\""
    usage
fi

if [[ "${region}" == "unset" ]]; then
    echo "ERROR: --region not specified"
    usage
fi
if [[ ! "${region}" =~ ^[a-z]+-[a-z]+-([a-z]+-)?[0-9]+$ ]]; then
    echo "ERROR: Not a valid value for --region: ${region}"
    usage
fi
if [[ ! -z "${wa_user}" ]] && [[ ! "${wa_user}" =~ ^[a-z_]([a-z0-9_-]{0,31}|[a-z0-9_-]{0,30}\$)$ ]]; then
    echo "ERROR: Not a valid value for --user: ${wa_user}"
    usage
fi

# --- VFS is not supported on macOS: fail loudly -------------------------------------
# DESIGN CHOICE: hard error (exit non-zero) rather than a silent warning, so that a mistaken
# VFS request surfaces immediately instead of producing a silently-degraded install. The
# dispatcher (__init__.py) also rejects --vfs-install-path on darwin before we ever get here;
# this is defense-in-depth for direct script invocation.
if [[ "${vfs_install_path}" != "unset" ]]; then
    echo "ERROR: The Deadline Virtual File System (VFS) is not supported on macOS."
    echo "       Remove the --vfs-install-path option and re-run the installer."
    exit 1
fi

# Determine the worker agent's PRIMARY group.
# CRITICAL SECURITY INVARIANT: the agent user's PRIMARY group must NOT be the job group. The job
# group is a SECONDARY membership only (see below). If the user already exists we read its current
# primary group; if we are creating the user we will give it a dedicated primary group named after
# the user (never the job group).
if user_exists "${wa_user}"; then
    wa_group=$(user_primary_group_name "${wa_user}")
    if [[ -z "${wa_group}" ]]; then
        # Fall back to the user name if the primary group name could not be resolved.
        wa_group="${wa_user}"
    fi
else
    # Newly created user -> its dedicated primary group has the same name as the user.
    wa_group="${wa_user}"
fi

# Default the job group if not provided via --group.
job_group=${job_group:-${default_job_group}}
if [[ ! -z "${job_group}" ]] && [[ ! "${job_group}" =~ ^[a-z_]([a-z0-9_-]{0,31}|[a-z0-9_-]{0,30}\$)$ ]]; then
    echo "ERROR: Not a valid value for --group: ${job_group}"
    usage
fi

banner
echo

# Output configuration
echo "Farm ID: ${farm_id}"
echo "Fleet ID: ${fleet_id}"
echo "Region: ${region}"
echo "Worker agent user: ${wa_user}"
echo "Worker agent group: ${wa_group}"
echo "Worker job group: ${job_group}"
echo "Scripts path: ${scripts_path}"
echo "Session root directory: ${session_root_dir}"
echo "Worker agent program path: ${worker_agent_program}"
echo "Allow worker agent shutdown: ${allow_shutdown}"
echo "Start launchd service: ${start_service}"
echo "Telemetry opt-out: ${telemetry_opt_out}"
echo "Disallow EC2 instance profile: ${disallow_instance_profile}"

# Confirmation prompt
if [ -z "$confirm" ]; then
    while :
    do
        read -p "Confirm install with the above settings (y/n):" confirm
        if [[ "${confirm}" == "y" ]]; then
            break
        elif [[ "${confirm}" == "n" ]]; then
            echo "Installation aborted"
            exit 1
        else
            echo "Not a valid choice (${confirm}). Please try again."
        fi
    done
fi

echo ""

# --- Create the worker agent user (hidden system account) ---------------------------
# DESIGN CHOICE: dscl (low-level) instead of `sysadminctl -addUser`.
#   * sysadminctl auto-assigns a UID >= 501 (a normal, login-visible account) and offers no
#     supported flag to force a hidden sub-500 system UID.
#   * dscl lets us explicitly set UniqueID (<500), IsHidden, NFSHomeDirectory, UserShell, and
#     PrimaryGroupID -- exactly what a headless daemon account needs, and it keeps the account
#     out of the login window.
# Idempotent: only create when the record is absent.
if ! user_exists "${wa_user}"; then
    echo "Creating worker agent user (${wa_user})"

    # First ensure the user's DEDICATED primary group exists (named after the user).
    # This group -- NOT the job group -- becomes the user's PrimaryGroupID (security invariant).
    if ! group_exists "${wa_user}"; then
        wa_primary_gid=$(allocate_system_id Groups PrimaryGroupID)
        dscl . -create /Groups/"${wa_user}"
        dscl . -create /Groups/"${wa_user}" PrimaryGroupID "${wa_primary_gid}"
        dscl . -create /Groups/"${wa_user}" RealName "${wa_user}"
    else
        wa_primary_gid=$(dscl . -read /Groups/"${wa_user}" PrimaryGroupID 2>/dev/null | awk '{print $2}')
    fi

    wa_uid=$(allocate_system_id Users UniqueID)
    dscl . -create /Users/"${wa_user}"
    dscl . -create /Users/"${wa_user}" UniqueID "${wa_uid}"
    dscl . -create /Users/"${wa_user}" PrimaryGroupID "${wa_primary_gid}"
    dscl . -create /Users/"${wa_user}" NFSHomeDirectory "${worker_agent_homedir}"
    # /usr/bin/false prevents interactive login (macOS equivalent of a nologin shell).
    dscl . -create /Users/"${wa_user}" UserShell /usr/bin/false
    dscl . -create /Users/"${wa_user}" RealName "AWS Deadline Cloud Worker Agent"
    # IsHidden=1 keeps a UID<500 account out of the login window / user pickers.
    dscl . -create /Users/"${wa_user}" IsHidden 1
    # Disable password auth entirely for this service account.
    # UNVERIFIED: on some macOS versions a service account also needs `dscl . -passwd` or an
    # AuthenticationAuthority reset; '*' matches the /etc/master.passwd disabled-account convention.
    dscl . -create /Users/"${wa_user}" Password '*'

    wa_group="${wa_user}"
    echo "Done creating worker agent user (${wa_user})"
else
    echo "Worker agent user ${wa_user} already exists"
fi

# --- Create the home directory (macOS does not auto-create it) ----------------------
if [[ ! -d "${worker_agent_homedir}" ]]; then
    echo "Creating worker agent home directory (${worker_agent_homedir})"
    mkdir -p "${worker_agent_homedir}"
fi
chown "${wa_user}:${wa_group}" "${worker_agent_homedir}"
chmod 750 "${worker_agent_homedir}"

# --- Create the job group -----------------------------------------------------------
# dseditgroup allocates a system GID and creates the group record. Idempotent via group_exists.
if ! group_exists "${job_group}"; then
    echo "Creating job group (${job_group})"
    dseditgroup -o create "${job_group}"
    echo "Done creating job group (${job_group})"
else
    echo "Job group ${job_group} already exists"
fi

# --- Enforce/verify the primary-group security invariant ----------------------------
# The job group must be a SECONDARY membership only, never the agent user's primary group.
current_primary_group=$(user_primary_group_name "${wa_user}")
if [[ "${current_primary_group}" == "${job_group}" ]]; then
    warning_lines+=(
        "The job group (${job_group}) is the primary group of worker agent user (${wa_user}). This is not a secure setup."
        "Consider re-installing and using a dedicated job group."
    )
else
    # Add the agent user to the job group as a SECONDARY member (Linux `usermod -a -G` equivalent).
    # `dseditgroup -o checkmember` reports current membership so this stays idempotent.
    if ! dseditgroup -o checkmember -m "${wa_user}" "${job_group}" &> /dev/null; then
        echo "Adding worker agent user (${wa_user}) to job group (${job_group})"
        dseditgroup -o edit -a "${wa_user}" -t user "${job_group}"
        echo "Done adding worker agent user (${wa_user}) to job group (${job_group})"
    else
        echo "Worker agent user (${wa_user}) is already in job group (${job_group})"
    fi
fi

# --- Sudoers configuration (--allow-shutdown) ---------------------------------------
# macOS shutdown binary lives at /sbin/shutdown (BSD shutdown). The Linux line used
# `/usr/sbin/shutdown now`; the BSD invocation is `/sbin/shutdown -h now` (-h = halt/power off).
# UNVERIFIED: confirm the worker agent actually invokes `/sbin/shutdown -h now` on macOS. The
# sudoers command MUST match the invoked argv EXACTLY or the NOPASSWD rule will not apply.
if [[ "${allow_shutdown}" == "yes" ]]; then
    echo "Setting up sudoers shutdown rule at /etc/sudoers.d/deadline-worker-shutdown"
    # /etc/sudoers.d exists and is included by default on macOS.
    mkdir -p /etc/sudoers.d
    cat > /etc/sudoers.d/deadline-worker-shutdown <<EOF
# Allow ${wa_user} user to shutdown the system
${wa_user} ALL=(root) NOPASSWD: /sbin/shutdown -h now
EOF
    chmod 440 /etc/sudoers.d/deadline-worker-shutdown
    echo "Done setting up sudoers shutdown rule"
elif [ -f /etc/sudoers.d/deadline-worker-shutdown ]; then
    echo "Removing sudoers shutdown rule at /etc/sudoers.d/deadline-worker-shutdown"
    rm /etc/sudoers.d/deadline-worker-shutdown
    echo "Done removing sudoers shutdown rule"
else
    echo "No prior sudoers shutdown rule at /etc/sudoers.d/deadline-worker-shutdown"
fi

# --- Directory provisioning (IDENTICAL to Linux: paths + modes are portable) --------
echo "Provisioning log directory (/var/log/amazon/deadline)"
mkdir -p /var/log/amazon/deadline
chmod 755 /var/log/amazon
chown -R "${wa_user}:${wa_group}" /var/log/amazon/deadline
chmod -R 750 /var/log/amazon/deadline
echo "Done provisioning log directory (/var/log/amazon/deadline)"

echo "Provisioning persistence directory (/var/lib/deadline)"
mkdir -p /var/lib/deadline/queues
mkdir -p /var/lib/deadline/credentials
chown "${wa_user}:${job_group}" \
    /var/lib/deadline \
    /var/lib/deadline/queues
chown "${wa_user}" /var/lib/deadline/credentials
chmod 750 \
    /var/lib/deadline \
    /var/lib/deadline/queues
chmod 700 \
    /var/lib/deadline/credentials
if [ -f /var/lib/deadline/worker.json ]; then
    chown "${wa_user}:${wa_group}" /var/lib/deadline/worker.json
    chmod 600 /var/lib/deadline/worker.json
fi
echo "Done provisioning persistence directory (/var/lib/deadline)"

echo "Provisioning root directory for OpenJD Sessions (${session_root_dir})"
mkdir -p "${session_root_dir}"
chown "${wa_user}:${job_group}" "${session_root_dir}"
chmod 755 "${session_root_dir}"
echo "Done provisioning root directory for OpenJD Sessions (${session_root_dir})"

echo "Provisioning configuration directory (/etc/amazon/deadline)"
mkdir -p /etc/amazon/deadline
chmod 750 /etc/amazon/deadline
cp "${SCRIPT_DIR}/worker.toml.example" /etc/amazon/deadline/
if [ ! -f /etc/amazon/deadline/worker.toml ]; then
    cp "${SCRIPT_DIR}/worker.toml.example" /etc/amazon/deadline/worker.toml
fi
chown -R "root:${wa_group}" /etc/amazon/deadline
chmod 640 /etc/amazon/deadline/worker.toml
echo "Done provisioning configuration directory"

# --- Write farm/fleet/region/session-root/instance-profile via the python config module
# IDENTICAL to Linux -- reuse the same module invocation and flags.
if [[ "${allow_shutdown}" == "yes" ]]; then
   shutdown_on_stop_flag="--shutdown-on-stop"
else
   shutdown_on_stop_flag="--no-shutdown-on-stop"
fi
if [[ "${disallow_instance_profile}" == "yes" ]]; then
   allow_ec2_instance_profile_flag="--no-allow-ec2-instance-profile"
else
   allow_ec2_instance_profile_flag="--allow-ec2-instance-profile"
fi

"${python_interpreter_path}"                \
    -m deadline_worker_agent.config         \
    --farm-id "${farm_id}"                  \
    --fleet-id "${fleet_id}"                \
    "${allow_ec2_instance_profile_flag}"    \
    "${shutdown_on_stop_flag}"              \
    --session-root-dir "${session_root_dir}" \
    --region "${region}"

# Telemetry opt-out (IDENTICAL to Linux). NOTE: uses `sed -i ''` (BSD sed requires an explicit
# empty extension argument for in-place editing, unlike GNU `sed -i`).
if [[ "${telemetry_opt_out}" == "yes" ]]; then
    echo "Opting out of telemetry collection"
    worker_config="/etc/amazon/deadline/worker.toml"
    if grep -q '^\[telemetry\]' "$worker_config" 2>/dev/null; then
        sed -i '' '/^\[telemetry\]/,/^\[/{s/^opt_out.*/opt_out = true/;}' "$worker_config"
        if ! grep -q '^opt_out' "$worker_config"; then
            sed -i '' '/^\[telemetry\]/a\
opt_out = true
' "$worker_config"
        fi
    else
        printf '\n[telemetry]\nopt_out = true\n' >> "$worker_config"
    fi
fi

# --- launchd LaunchDaemon (replaces the systemd unit) -------------------------------
if ! [[ "${no_install_service}" == "yes" ]]; then
    echo "Installing launchd LaunchDaemon to ${launchd_plist}"

    # Split the program path into ProgramArguments array elements. worker_agent_program is a bare
    # path with no arguments, so this yields a single-element array.
    # UNVERIFIED: assumes the program path contains no whitespace (true for standard scripts-path).
    prog_args_xml=""
    for token in ${worker_agent_program}; do
        prog_args_xml+="        <string>${token}</string>"$'\n'
    done

    # RunAtLoad controls whether the daemon runs immediately when it is bootstrapped and on
    # every boot. We gate it on --start so macOS matches the Linux installer's behavior: without
    # --start the daemon is registered but not started now (Linux runs `systemctl enable` only),
    # and with --start it starts immediately and on boot (Linux runs `systemctl start` too).
    if [[ "${start_service}" == "yes" ]]; then
        run_at_load="true"
    else
        run_at_load="false"
    fi

    # NOTE ON MAPPINGS from the systemd unit:
    #   User=                 -> UserName
    #   WorkingDirectory=     -> WorkingDirectory
    #   Environment=AWS_*     -> EnvironmentVariables dict
    #   ExecStart=            -> ProgramArguments (array)
    #   Restart=on-failure    -> KeepAlive { SuccessfulExit = false }  (restart only on failure)
    #   WantedBy=multi-user.target + (systemctl start when --start) -> RunAtLoad, gated on --start
    #   StandardOutput/Error=null  -> StandardOutPath/StandardErrorPath = /dev/null
    #   AmbientCapabilities=CAP_KILL -> OMITTED. No macOS equivalent. The worker agent runtime
    #       already falls back to `pgrep` + `sudo kill` for process cleanup on non-Linux platforms,
    #       so no ambient capability is required here.
    #   VFS env vars (FUS3_PATH/DEADLINE_VFS_PATH) -> OMITTED. VFS is unsupported on macOS.
    cat > "${launchd_plist}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>${launchd_label}</string>
    <key>UserName</key>
    <string>${wa_user}</string>
    <key>WorkingDirectory</key>
    <string>${worker_agent_homedir}</string>
    <key>ProgramArguments</key>
    <array>
${prog_args_xml}    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>AWS_REGION</key>
        <string>${region}</string>
        <key>AWS_DEFAULT_REGION</key>
        <string>${region}</string>
    </dict>
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>
    <key>RunAtLoad</key>
    <${run_at_load}/>
    <key>StandardOutPath</key>
    <string>/dev/null</string>
    <key>StandardErrorPath</key>
    <string>/dev/null</string>
</dict>
</plist>
EOF

    # launchd REJECTS a plist that is group- or other-writable, so mode is 644 (NOT 640 like the
    # Linux systemd unit). Ownership must be root:wheel.
    chown root:wheel "${launchd_plist}"
    chmod 644 "${launchd_plist}"
    echo "Done installing launchd LaunchDaemon"

    # Idempotent (re)load: bootout an already-loaded instance before bootstrap, otherwise
    # `bootstrap` fails with "service already bootstrapped".
    # UNVERIFIED: `bootout` returns non-zero if the service is not loaded; we tolerate that with `|| true`.
    if launchctl print "system/${launchd_label}" &> /dev/null; then
        echo "Existing LaunchDaemon detected; unloading before reload"
        launchctl bootout system "${launchd_plist}" &> /dev/null || true
    fi

    # bootstrap loads the daemon and enables start-on-boot; enable keeps it eligible to run.
    # Whether it also starts *now* is governed by RunAtLoad (gated on --start above), matching
    # the Linux installer where `systemctl enable` always runs but `systemctl start` is --start-only.
    echo "Bootstrapping and enabling the LaunchDaemon"
    launchctl bootstrap system "${launchd_plist}"
    launchctl enable "system/${launchd_label}"

    if [[ "${start_service}" == "yes" ]]; then
        # RunAtLoad=true means bootstrap already started it; kickstart -k guarantees an immediate
        # (re)start even if bootstrap raced or the service was previously loaded.
        echo "Starting the service"
        launchctl kickstart -k "system/${launchd_label}"
        echo "Done starting the service"
    fi
fi

echo "Done"

# Output warning lines if any
if [ ${#warning_lines[@]} -gt 0 ]; then
    echo
    echo "!!!! WARNING !!!"
    echo
    for i in "${!warning_lines[@]}"; do
        echo "${warning_lines[i]}"
    done
    echo
fi

# UNVERIFIED (macOS platform integration, not scriptable here -- operator checklist):
#   * TCC / Full Disk Access: a headless LaunchDaemon may be blocked by TCC from protected paths
#     (Desktop/Documents/removable volumes) and cannot present the consent UI. Fleets likely need
#     an MDM PPPC profile granting Full Disk Access to the ${worker_agent_program} binary.
#   * Code signing / Gatekeeper: an unsigned/unnotarized agent binary may be quarantined. Ensure
#     the binary is signed + notarized (or delivered without the com.apple.quarantine xattr).
