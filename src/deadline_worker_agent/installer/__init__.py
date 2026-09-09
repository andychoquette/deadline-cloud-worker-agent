# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations
from typing import Optional, Any
from argparse import ArgumentParser, Namespace
from pathlib import Path
from subprocess import CalledProcessError, run
import re
import requests
import sys
import sysconfig
import time

from deadline_worker_agent.config.settings import (
    DEFAULT_MACOS_SESSION_ROOT_DIR,
    DEFAULT_POSIX_SESSION_ROOT_DIR,
    DEFAULT_WINDOWS_SESSION_ROOT_DIR,
)


if sys.platform == "win32":
    from deadline_worker_agent.installer.win_installer import (
        start_windows_installer,
        InstallerFailedException,
    )


INSTALLER_PATH = {
    "linux": Path(__file__).parent / "install.sh",
    "darwin": Path(__file__).parent / "install_macos.sh",
}


_IMDS_REQUEST_TIMEOUT = (1.0, None)
"""Ceiling on the *connect* phase of an IMDS request during install.

The metadata address is link-local, so where nothing answers it the connect waits on ARP
rather than being refused. Unbounded, that stalls the installer with no output on every
workstation install, which is the whole reason for this value.

Read is deliberately left unbounded, matching bootstrap.IMDS_REQUEST_TIMEOUT and this
function's behaviour before the connect bound existed. `None` from _get_ec2_region is fatal --
`install()` prints an error and exits 1 -- and a user-data install runs during boot, when IMDS
is slowest. A read ceiling would make a consistently-slow-but-present IMDS fail the install,
and the retry does not cover it: three attempts at an N-second read tolerate an IMDS slower
than N no better than one does. Waiting is the correct behaviour there."""

_IMDS_ATTEMPTS = 3
"""Attempts before giving up on detecting a region from IMDS.

Retried because `None` here is fatal: `install()` prints an error and exits 1 when no
--region was passed. Bounding the request without retrying it would turn a slow EC2 answer
into a failed install, and user-data installs run during boot."""

_IMDS_BACKOFF_S = 0.5
"""Delay between region-detection attempts.

A throttled IMDS answers immediately, so back-to-back retries would meet the same empty
token bucket."""


def _is_transient_status(status_code: int) -> bool:
    """Whether an IMDS status may clear on its own.

    5xx belongs here as much as 429 does, and for the same reason _IMDS_ATTEMPTS exists at
    all: a user-data install runs during boot, which is when the metadata service is not yet
    fully up and answers 500 or 503. Treating those as final would leave the retry covering
    only the narrower half of the transient set it was added for.
    """
    return status_code == 429 or status_code >= 500


class _ImdsTransientError(Exception):
    """IMDS answered in a way another attempt may improve on.

    Raised for a transient status per _is_transient_status, and for a 401 on the
    availability-zone hop. Every other outcome is determinate -- IMDSv2 disabled, a hop limit
    too low, an empty token, an empty or unparseable AZ, or a ConnectTimeout from a
    black-holed address -- and retrying those only repeats the same message at the user.

    Not raised for a slow read. The read half of _IMDS_REQUEST_TIMEOUT is unbounded, so a
    present-but-slow IMDS is waited out rather than retried; there is no ReadTimeout to catch.
    Anyone bounding that read later needs to add the clause as well, because the retry does not
    already cover it.

    Must be re-raised past the broad `except Exception` in _get_ec2_region_once, which would
    otherwise convert a retryable case back into a determinate answer.
    """


def _get_ec2_region() -> Optional[str]:
    """
    Gets the AWS region if running on EC2 by querying IMDS.
    Returns None if region could not be detected.
    """
    for attempt in range(1, _IMDS_ATTEMPTS + 1):
        try:
            return _get_ec2_region_once()
        except _ImdsTransientError as e:
            print(f"Failed to detect AWS region: {e}")
            if attempt < _IMDS_ATTEMPTS:
                print(f"Retrying region detection ({attempt} of {_IMDS_ATTEMPTS} attempts used)")
                time.sleep(_IMDS_BACKOFF_S)
    return None


def _get_ec2_region_once() -> Optional[str]:
    """One attempt at reading the region from IMDS. See _get_ec2_region.

    Returns None when the answer is determinate -- there is no region to be had, and another
    attempt would say the same. Raises _ImdsTransientError for the cases a further attempt may
    resolve; see that class for which those are.
    """
    try:
        # Create IMDSv2 token
        token_response = requests.put(
            url="http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "10"},  # 10 second expiry
            timeout=_IMDS_REQUEST_TIMEOUT,
        )
        # Checked before the body is used. A 403 (IMDSv2 disabled) or 429 (throttled) has a
        # non-empty error body, so `if not token` does not catch it -- that body would be
        # sent as the token, IMDS would answer the next hop 401, and the user would be told
        # the availability zone was unexpected while quoting an error page at them.
        if _is_transient_status(token_response.status_code):
            raise _ImdsTransientError(
                f"IMDS returned HTTP {token_response.status_code} for the token request"
            )
        if token_response.status_code != 200:
            print(
                "AWS region could not be detected: IMDS returned HTTP "
                f"{token_response.status_code} for an IMDSv2 token. IMDSv2 may be disabled, "
                "or the hop limit may be too low to reach it from here."
            )
            return None

        token = token_response.text
        if not token:
            raise RuntimeError("Received empty IMDSv2 token")

        # Get AZ
        az_response = requests.get(
            url="http://169.254.169.254/latest/meta-data/placement/availability-zone",
            headers={"X-aws-ec2-metadata-token": token},
            timeout=_IMDS_REQUEST_TIMEOUT,
        )
        # Checked for the same reason as the token, since throttling is applied per request:
        # this hop can be rejected after the token succeeded, and the 10s token TTL can also
        # lapse between the two calls. Unchecked, that error body lands in `az`, fails the
        # regex below, and reports "unexpected availability zone" with an error page in it --
        # the outcome the token check exists to avoid, reached one hop later. A 429 also
        # arrives as a body rather than an exception, so without this it would be determinate
        # and skip the retry it deserves.
        # 401 is transient on this hop only. It means the token was rejected, and the 10s TTL
        # requested above can lapse between the two calls -- a fresh token on the next attempt
        # is exactly the fix. A 401 on the token *request* would not be re-obtainable, which is
        # why that branch does not include it.
        if _is_transient_status(az_response.status_code) or az_response.status_code == 401:
            raise _ImdsTransientError(
                f"IMDS returned HTTP {az_response.status_code} for the availability zone"
            )
        if az_response.status_code != 200:
            print(
                "AWS region could not be detected: IMDS returned HTTP "
                f"{az_response.status_code} for the availability zone."
            )
            return None

        az = az_response.text
    except _ImdsTransientError:
        # Raised above for a transient status. Re-raised rather than swallowed by the broad
        # handler below, which would turn a retryable case into a determinate answer.
        #
        # No ReadTimeout clause: the read is unbounded, so a slow-but-present IMDS is waited
        # out rather than retried. A ConnectTimeout -- the black-holed address on a
        # workstation -- falls to the handler below and is determinate, which is right: no
        # number of attempts will find a region there.
        raise
    except Exception as e:
        print(f"Failed to detect AWS region: {e}")
        return None
    else:
        if not az:
            print("AWS region could not be detected, received empty response from IMDS")
            return None

        match = re.match(r"^([a-z-]+-[0-9])([a-z])?$", az)
        if not match:
            print(
                f"AWS region could not be detected, got unexpected availability zone from IMDS: {az}"
            )
            return None

        return match.group(1)


def install() -> None:
    """Installer entrypoint for the AWS Deadline Cloud Worker Agent"""

    if sys.platform not in ["linux", "darwin", "win32"]:
        print(f"ERROR: Unsupported platform {sys.platform}")
        sys.exit(1)

    arg_parser = get_argument_parser()
    args = arg_parser.parse_args(namespace=ParsedCommandLineArguments())
    scripts_path = Path(sysconfig.get_path("scripts"))

    # The Deadline Virtual File System (VFS) is not supported on macOS. Reject the option here
    # so the error surfaces before we shell out to install_macos.sh (which also rejects it).
    if sys.platform == "darwin" and args.vfs_install_path:
        print("ERROR: --vfs-install-path is not supported on macOS.")
        sys.exit(1)

    if args.region is None:
        args.region = _get_ec2_region()
        if args.region is None:
            print("ERROR: Unable to detect AWS region. Please provide a value for --region.")
            sys.exit(1)

    if sys.platform == "win32":
        installer_args: dict[str, Any] = dict(
            farm_id=args.farm_id,
            fleet_id=args.fleet_id,
            region=args.region,
            install_service=args.install_service,
            start_service=args.service_start,
            confirm=args.confirmed,
            allow_shutdown=args.allow_shutdown,
            parser=arg_parser,
            grant_required_access=args.grant_required_access,
            allow_ec2_instance_profile=not args.disallow_instance_profile,
            session_root_dir=args.session_root_dir,
        )
        if args.user:
            installer_args.update(user_name=args.user)
        if args.group:
            installer_args.update(group_name=args.group)
        if args.password:
            installer_args.update(password=args.password)
        if args.telemetry_opt_out:
            installer_args.update(telemetry_opt_out=args.telemetry_opt_out)
        if args.windows_job_user:
            installer_args.update(windows_job_user=args.windows_job_user)

        try:
            start_windows_installer(**installer_args)
        except InstallerFailedException as e:
            print(f"ERROR: {e}")
            sys.exit(1)
    else:
        cmd = [
            "sudo",
            str(INSTALLER_PATH[sys.platform]),
            "--farm-id",
            args.farm_id,
            "--fleet-id",
            args.fleet_id,
            "--region",
            args.region,
            "--user",
            args.user,
            "--scripts-path",
            str(scripts_path),
            "--python-interpreter-path",
            sys.executable,
            "--session-root-dir",
            str(args.session_root_dir),
        ]
        if args.vfs_install_path:
            cmd += ["--vfs-install-path", args.vfs_install_path]
        if args.group:
            cmd += ["--group", args.group]
        if args.confirmed:
            cmd.append("-y")
        if args.service_start:
            cmd.append("--start")
        if args.allow_shutdown:
            cmd.append("--allow-shutdown")
        if not args.install_service:
            cmd.append("--no-install-service")
        if args.telemetry_opt_out:
            cmd.append("--telemetry-opt-out")
        if args.disallow_instance_profile:
            cmd.append("--disallow-instance-profile")

        try:
            run(
                cmd,
                check=True,
            )
        except CalledProcessError as error:
            sys.exit(error.returncode)


class ParsedCommandLineArguments(Namespace):
    """Represents the parsed installer command-line arguments"""

    farm_id: str
    fleet_id: str
    region: Optional[str] = None
    user: str
    password: Optional[str] = None
    group: Optional[str] = None
    confirmed: bool
    service_start: bool
    allow_shutdown: bool
    install_service: bool
    telemetry_opt_out: bool
    vfs_install_path: str
    grant_required_access: bool
    disallow_instance_profile: bool
    windows_job_user: Optional[str] = None
    session_root_dir: Path


def get_argument_parser() -> ArgumentParser:  # pragma: no cover
    """Returns a command-line argument parser for the AWS Deadline Cloud Worker Agent"""

    parser = ArgumentParser(
        prog="install-deadline-worker",
        description="Installer for the AWS Deadline Cloud Worker Agent",
    )
    parser.add_argument(
        "--farm-id",
        help="The AWS Deadline Cloud Farm ID that the Worker belongs to.",
        required=True,
    )
    parser.add_argument(
        "--fleet-id",
        help="The AWS Deadline Cloud Fleet ID that the Worker belongs to.",
        required=True,
    )
    parser.add_argument(
        "--region",
        help=(
            "The AWS region of the AWS Deadline Cloud farm. "
            "If on EC2, this is optional and the region will be automatically detected. Otherwise, this option is required."
        ),
        default=None,
    )

    # Windows local usernames are restricted to 20 characters in length.
    default_username = "deadline-worker-agent" if sys.platform != "win32" else "deadline-worker"
    parser.add_argument(
        "--user",
        help=f'The username of the AWS Deadline Cloud Worker Agent user. Defaults to "{default_username}".',
        default=default_username,
    )

    parser.add_argument(
        "--group",
        help='The group that is shared between the Agent user and the user(s) that jobs run as. Defaults to "deadline-job-users".',
    )
    parser.add_argument(
        "--start",
        help="Starts the service immediately. Defaults to start on system boot. This option is ignored if --no-install-service is used.",
        action="store_true",
        dest="service_start",
    )

    if sys.platform == "win32":
        help = "Controls whether to grant the worker agent OS user the privilege to shutdown the system"
    else:
        help = "Controls whether to create/delete a sudoers rule allowing the worker agent OS user to shutdown the system"
    parser.add_argument(
        "--allow-shutdown",
        help=help,
        action="store_true",
    )

    parser.add_argument(
        "--no-install-service",
        help="Skips the worker agent service installation",
        action="store_false",
        dest="install_service",
    )
    parser.add_argument(
        "--telemetry-opt-out",
        help="Opts out of telemetry data collection",
        action="store_true",
    )
    parser.add_argument(
        "--yes",
        "-y",
        help="Confirms the installation and skips the interactive confirmation prompt.",
        action="store_true",
        dest="confirmed",
    )
    parser.add_argument(
        "--vfs-install-path",
        help="Absolute path for the install location of the deadline vfs.",
    )
    parser.add_argument(
        "--disallow-instance-profile",
        help=(
            "Disallow running the worker agent with an EC2 instance profile. When this is provided, the worker "
            "agent makes requests to the EC2 instance meta-data service (IMDS) to check for an instance profile. "
            "If an instance profile is detected, the worker agent will stop and exit. When this is not provided, "
            "the worker agent no longer performs these checks, allowing it to run with an EC2 instance profile."
        ),
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--session-root-dir",
        help="The root directory under which the worker agent creates session directories",
        type=Path,
        default=(
            str(DEFAULT_WINDOWS_SESSION_ROOT_DIR)
            if sys.platform == "win32"
            else (
                str(DEFAULT_MACOS_SESSION_ROOT_DIR)
                if sys.platform == "darwin"
                else str(DEFAULT_POSIX_SESSION_ROOT_DIR)
            )
        ),  # pragma: nocover
    )

    if sys.platform == "win32":
        parser.add_argument(
            "--password",
            help=(
                "The password for the AWS Deadline Cloud Worker Agent user. Defaults to generating a password "
                "if the user does not exist or prompting for the password if the user pre-exists."
            ),
            required=False,
            default=None,
        )
        parser.add_argument(
            "--grant-required-access",
            help=(
                "Allows the installer to modify an existing user so that it can successfully run the worker agent. This will allow "
                "the installer to add the user to the Administrators group and grant any missing user rights which are required to "
                "run the worker agent. This option has no effect if a new user is created by the installer."
            ),
            action="store_true",
            required=False,
            default=False,
        )
        parser.add_argument(
            "--windows-job-user",
            help=(
                "The username of the Windows user that jobs run as. The password for this user account is reset during worker startup."
            ),
            required=False,
            default=None,
        )

    return parser
