# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from __future__ import annotations

import json
import signal
import os
import sys
import traceback
from contextlib import nullcontext
from concurrent.futures import Executor, Future, ThreadPoolExecutor, wait
from datetime import datetime, timedelta, timezone
from logging import getLogger
from threading import Event
from types import FrameType
from typing import Any, NamedTuple, cast
from pathlib import Path

import boto3
import requests

from .aws_credentials import WorkerBoto3Session, AwsCredentialsRefresher
from .boto import DeadlineClient
from .config import JobsRunAsUserOverride
from ._session_runtime_kind import SessionRuntimeKind
from .errors import ServiceShutdown
from .log_messages import AwsCredentialsLogEvent, AwsCredentialsLogEventOp
from .metrics import HostMetricsLogger
from .scheduler import WorkerScheduler
from .sessions import Session


logger = getLogger(__name__)


def _is_usable_imds_status(status_code: int) -> bool:
    """Whether an IMDS status carries an answer the caller can act on.

    404 is included because it is the *normal* reply on both endpoints the shutdown monitor
    reads: /spot/instance-action returns it when no interruption is pending, and
    /autoscaling/target-lifecycle-state when the host is not in an auto-scaling group. Treating
    every non-200 as a failure would report a healthy worker as unanswered and, because that
    state suppresses repeats, would then stay quiet through a real outage.
    """
    return status_code in (200, 404)


class WorkerSessionCollection:
    def __init__(self, *, worker: Worker) -> None: ...

    def __getitem__(self, session_id: str) -> Session:
        raise NotImplementedError()

    def create(self, *, id: str) -> Session:
        raise NotImplementedError("WorkerSessionCollection.create() method not implemented")


class WorkerShutdown(NamedTuple):
    """An error indicating that the Worker is shutting down"""

    grace_time: timedelta
    """The amount of grace time before the Worker Node will shutdown"""

    fail_message: str
    """A human-friendly message explaining the shutdown"""


class Worker:
    _EC2_SHUTDOWN_MONITOR_RATE = timedelta(seconds=1)
    """The rate that the Worker polls for EC2 instance termination notifications (spot interruption
    an Auto-scaling life-cycle events)"""

    _ASG_LIFECYCLE_SHUTDOWN_GRACE = timedelta(minutes=2)
    """The amount of time to allow the Worker to gracefully shutdown after detecting an auto-scaling
    life-cycle event."""

    _IMDS_REQUEST_TIMEOUT = (1.0, 1.0)
    """Ceiling on a single IMDS request, as (connect, read).

    The metadata address is link-local, so where nothing answers it the connect waits on ARP
    rather than being refused and an unbounded request parks the calling thread. A tuple
    because `requests` applies a scalar to each phase separately, so the pair is what makes
    the per-attempt cost readable. See TestImdsProbeCost for what the pair costs per attempt."""

    _IMDS_PROBE_ATTEMPTS = 3
    """Attempts before concluding a host is not on EC2.

    Retried because the verdict is permanent -- see _is_ec2_host."""

    _IMDS_CAUSE_SLOW_READ = "the connection was accepted but nothing came back in time"
    """Cause phrase for a read timeout: IMDS is present and answering, just not in time."""

    _IMDS_CAUSE_NO_CONNECT = (
        "no connection was accepted, though the token request answered moments ago"
    )
    """Cause phrase for a connect timeout, which means the opposite of a read timeout."""

    _IMDS_PROBE_BACKOFF_S = 0.5
    """Delay between probe attempts.

    A throttled IMDS answers immediately with a 429, so back-to-back retries would meet the
    same empty token bucket and cover only the slow case."""

    _farm_id: str
    _fleet_id: str
    _worker_id: str
    _scheduler: WorkerScheduler
    _executor: Executor
    _stop: Event
    _deadline_client: DeadlineClient
    _s3_client: boto3.client
    _logs_client: boto3.client
    _boto_session: WorkerBoto3Session
    _worker_persistence_dir: Path
    _host_metrics_logger: HostMetricsLogger | None = None
    _retain_session_dir: bool
    _imds_unanswered: set[str]

    def __init__(
        self,
        *,
        farm_id: str,
        fleet_id: str,
        worker_id: str,
        deadline_client: DeadlineClient,
        s3_client: boto3.client,
        logs_client: boto3.client,
        boto_session: WorkerBoto3Session,
        job_run_as_user_override: JobsRunAsUserOverride,
        cleanup_session_user_processes: bool,
        worker_persistence_dir: Path,
        worker_logs_dir: Path | None,
        session_root_dir: Path,
        host_metrics_logging: bool,
        host_metrics_logging_interval_seconds: float | None = None,
        retain_session_dir: bool = False,
        session_runtime_kind: SessionRuntimeKind = SessionRuntimeKind.PYTHON,
        stop: Event | None = None,
    ) -> None:
        self._deadline_client = deadline_client
        self._s3_client = s3_client
        self._logs_client = logs_client
        self._executor = ThreadPoolExecutor(
            max_workers=3,
            thread_name_prefix="Worker",
        )
        self._farm_id = farm_id
        self._fleet_id = fleet_id
        self._worker_id = worker_id
        self._scheduler = WorkerScheduler(
            deadline=deadline_client,
            farm_id=farm_id,
            fleet_id=fleet_id,
            worker_id=worker_id,
            job_run_as_user_override=job_run_as_user_override,
            boto_session=boto_session,
            cleanup_session_user_processes=cleanup_session_user_processes,
            worker_persistence_dir=worker_persistence_dir,
            worker_logs_dir=worker_logs_dir,
            retain_session_dir=retain_session_dir,
            session_runtime_kind=session_runtime_kind,
            stop=stop,
            session_root_dir=session_root_dir,
        )
        self._stop = stop or Event()
        self._boto_session = boto_session
        self._worker_persistence_dir = worker_persistence_dir
        self._retain_session_dir = retain_session_dir
        # The IMDS paths whose last query did not answer, keyed individually. Gates the
        # warning in _log_imds_unanswered to the transition, per path -- one shared flag
        # would flap, because the monitor queries two paths per poll and either can time out
        # while the other answers.
        self._imds_unanswered = set()

        if host_metrics_logging:
            assert host_metrics_logging_interval_seconds is not None, (
                "host_metrics_logging_interval_seconds is required if host metrics logging is enabled"
            )
            self._host_metrics_logger = HostMetricsLogger(
                logger=logger, interval_s=host_metrics_logging_interval_seconds
            )

        if os.name == "posix":
            signal.signal(signal.SIGTERM, self._signal_handler)
            signal.signal(signal.SIGINT, self._signal_handler)
            # TODO: Remove this once WA is stable or put behind a debug flag
            signal.signal(signal.SIGUSR1, self._output_thread_stacks)  # type: ignore
        elif os.name == "nt":
            from .windows.win_session import is_windows_session_zero

            # If we are in session 0, we are running as a Windows Service using pywin32
            # pywin32's pythonservice.exe owns the main thread and the Python application
            # appears to run on a secondary thread. Python only allows registering signal
            # handlers on the main thread and we only need them in the interactive case
            # anyways
            if not is_windows_session_zero():
                signal.signal(signal.SIGTERM, self._signal_handler)
                signal.signal(signal.SIGINT, self._signal_handler)
                signal.signal(signal.SIGBREAK, self._signal_handler)  # type: ignore[attr-defined]

    def _signal_handler(self, signum: int, frame: FrameType | None = None) -> None:
        """
        Signal handler for SIGTERM/SIGINT that intercepts the signals and lets the worker
        gracefully wind-down what it's currently doing.
        This will set the _interrupted flag to True when we get such a signal.
        """
        if (
            signum in (signal.SIGTERM, signal.SIGINT)
            or
            # This is to relax mypy since signal.SIGBREAK is only defined on Windows
            (sys.platform == "win32" and signum == getattr(signal, "SIGBREAK"))
        ):
            logger.info(f"Received signal {signum}. Initiating application shutdown.")
            self._interrupted = True
            self._scheduler.shutdown(
                grace_time=timedelta(seconds=4),
                fail_message=f"Worker Agent received OS signal {signum}",
            )
            self._stop.set()

    # TODO: Remove this once WA is stable or put behind a debug flag
    def _output_thread_stacks(self, signum: int, frame: FrameType | None = None) -> None:
        """
        Signal handler for SIGUSR1

        This signal is designated for application-defined behaviors. In our case, we want to output
        stack traces for all running threads.
        """
        if signum in (signal.SIGUSR1,):  # type: ignore
            logger.info(f"Received signal {signum}. Initiating application shutdown.")
            # OUTPUT STACK TRACE FOR ALL THREADS
            print("\n*** STACKTRACE - START ***\n", file=sys.stderr)
            code = []
            for threadId, stack in sys._current_frames().items():
                code.append("\n# ThreadID: %s" % threadId)
                for filename, lineno, name, line in traceback.extract_stack(stack):
                    code.append('File: "%s", line %d, in %s' % (filename, lineno, name))
                    if line:
                        code.append("  %s" % (line.strip()))

            for line in code:
                print(line, file=sys.stderr)
            print("\n*** STACKTRACE - END ***\n", file=sys.stderr)

    @property
    def id(self) -> str:
        raise NotImplementedError("Worker.id property not implemented")

    @property
    def sessions(self) -> WorkerSessionCollection:
        raise NotImplementedError("Worker.sessions property not implemented")

    def run(self) -> None:
        """Runs the main Worker loop for processing sessions."""

        monitor_ec2_shutdown: Future[WorkerShutdown | None] | None = None
        with (
            self._executor,
            AwsCredentialsRefresher(
                resource={"resource": self._worker_id},
                session=self._boto_session,
                failure_callback=self._aws_credentials_refresh_failure,
            ),
            self._host_metrics_logger or nullcontext(),
        ):
            scheduler_future = self._executor.submit(self._scheduler.run)
            futures: list[Future[Any]] = [
                scheduler_future,
            ]
            try:
                # Inside the try, not before it. The scheduler is already running on the
                # executor at this point, so anything raised here would leave the `with` and
                # have ThreadPoolExecutor.__exit__ join a scheduler that was never asked to
                # stop -- an unkillable worker, since the signal handler is not what is
                # blocked. The handler below is the cleanup that prevents it: it sets
                # self._stop and shuts the scheduler down before re-raising.
                if self._is_ec2_host():
                    # Create a future for monitoring EC2 shutdown events
                    monitor_ec2_shutdown = self._executor.submit(self._monitor_ec2_shutdown)
                    futures.append(monitor_ec2_shutdown)

                complete_futures, _ = wait(
                    fs=futures,
                    return_when="FIRST_COMPLETED",
                )
            except BaseException as e:
                logger.exception(e)
                logger.info("Shutting down scheduler...")
                self._scheduler.shutdown(
                    grace_time=timedelta(seconds=5),
                    fail_message=f"Worker Agent encountered error: {e}",
                )
                logger.info("Shutting down monitoring threads...")
                self._stop.set()
                raise
            else:
                for future in complete_futures:
                    if monitor_ec2_shutdown and future is monitor_ec2_shutdown:
                        logger.debug("monitor ec2 shutdown future complete")
                        worker_shutdown: WorkerShutdown | None = future.result()
                        # We only stop the other threads if we detected an imminent EC2 shutdown.
                        # The monitoring thread returns None if the monitor thread was stopped by the OS signal handler
                        if worker_shutdown:
                            self._stop.set()
                            self._scheduler.shutdown(
                                grace_time=worker_shutdown.grace_time,
                                fail_message=worker_shutdown.fail_message,
                            )
                        else:
                            # If we are here, it's because self._stop.set() was set causing the monitor_ec2_shutdown thread to join.
                            # The scheduler thread has a longer wait, so let's wake it up so it can join as well.
                            self._scheduler.shutdown(
                                fail_message="The Worker received a shutdown event locally from the host machine."
                            )
                    elif future is scheduler_future:
                        logger.debug("scheduler future complete")
                        try:
                            future.result()
                        except ServiceShutdown:
                            # Suppress logging
                            raise
                        except Exception as e:
                            logger.exception(e)
                            raise
                        finally:
                            self._stop.set()
                    else:
                        raise NotImplementedError(f"Future not handled {future}")
            logger.debug("Waiting for threads to join...")
        logger.info("Worker shutdown complete")

    def _aws_credentials_refresh_failure(self, exception: Exception) -> None:
        """Called when we fail to refresh the Worker Agent's AWS Credentials.
        The given exception will be either:
        1) TimeoutException - indicating that the credentials are either expired or
            will expire soon. args[0] of the exception is a UTC datetime indicating when
            the credentials will expire.
        2) DeadlineRequestError/DeadlineRequestUnrecoverableError - Indicating that
            we encountered a fatal error trying to refresh the credentials (e.g. the Fleet
            Role is missing permissions to refresh itself).

        In either case, we initiate a scheduler shutdown.
        """
        if isinstance(exception, TimeoutError):
            expiry_time = cast(datetime, exception.args[0])
            time_remaining = datetime.now(timezone.utc) - expiry_time
            if time_remaining < timedelta(minutes=0):
                logger.critical(
                    AwsCredentialsLogEvent(
                        op=AwsCredentialsLogEventOp.EXPIRED,
                        resource=self._worker_id,
                        message="AWS Credentials have expired!",
                    )
                )
                grace_time = timedelta(seconds=5)
                fail_message = "Worker AWS Credentials have expired!"
            else:
                logger.error(
                    AwsCredentialsLogEvent(
                        op=AwsCredentialsLogEventOp.REFRESH,
                        resource=self._worker_id,
                        message="Worker AWS Credentials could not be refreshed. They will expire soon.",
                        expiry=expiry_time.isoformat(),
                    )
                )
                grace_time = time_remaining
                fail_message = "Worker AWS Credentials are expiring and cannot be refreshed."
        else:
            # exception is: DeadlineRequestError or DeadlineRequestUnrecoverableError
            grace_time = timedelta(seconds=30)
            fail_message = "Fatal error refreshing Worker AWS Credentials. See log for details."
            logger.critical(
                AwsCredentialsLogEvent(
                    op=AwsCredentialsLogEventOp.REFRESH,
                    resource=self._worker_id,
                    message="Fatal error refreshing Worker AWS Credentials: %s" % str(exception),
                )
            )
        self._stop.set()
        self._scheduler.shutdown(grace_time=grace_time, fail_message=fail_message)

    def _monitor_ec2_shutdown(self) -> WorkerShutdown | None:
        """Monitors for external shutdown events.

        This includes:
        1.  EC2 spot interruptions
        2.  EC2 auto-scaling life-cycle scale-in events

        This is a synchronous blocking call, so it should be run as a future.

        Returns
        -------
        WorkerShutdown | None
            An optional WorkerShutdown which specifies the amount of grace time before the shutdown
            occurs and a human-friendly message describing the shutdown reason.
        """
        monitor_ec2_shutdown_rate = Worker._EC2_SHUTDOWN_MONITOR_RATE.total_seconds()
        while not self._stop.wait(timeout=monitor_ec2_shutdown_rate):
            if not (imdsv2_token := self._get_ec2_metadata_imdsv2_token()):
                # Reaching here means IMDS answered at startup -- _is_ec2_host gated this
                # thread on it -- and has now stopped, so this is the same condition the two
                # queries below warn about rather than "not on EC2". Routed through the same
                # per-path suppression: unconditional at this level it fires once a second and
                # buries those warnings, being the first thing every failing poll logs.
                self._log_imds_unanswered("api/token", cause="the request returned no token")
                logger.debug(
                    "IMDS unavailable - unable to monitor for spot interruption or ASG life-cycle "
                    "changes"
                )
                continue
            self._imds_answered("api/token")

            # Check for spot interruption or shutdown
            if (
                spot_shutdown_grace := self._get_spot_instance_shutdown_action_timeout(
                    imdsv2_token=imdsv2_token
                )
            ) is not None:
                logger.info("Spot interruption detected. Termination in %s", spot_shutdown_grace)
                return WorkerShutdown(
                    grace_time=spot_shutdown_grace,
                    fail_message="The Worker received an EC2 spot interruption",
                )
            elif self._is_asg_terminated(imdsv2_token=imdsv2_token):
                logger.info(
                    "Auto-scaling life-cycle change event detected. Termination in %s",
                    Worker._ASG_LIFECYCLE_SHUTDOWN_GRACE,
                )
                return WorkerShutdown(
                    grace_time=Worker._ASG_LIFECYCLE_SHUTDOWN_GRACE,
                    fail_message="The Worker received an auto-scaling life-cycle change event",
                )

        logger.debug("EC2 shutdown monitoring thread exited")

        return None

    def _is_ec2_host(self) -> bool:
        """Whether to monitor for EC2 spot interruption and ASG lifecycle changes.

        Retried, unlike a bare token query: the verdict is taken once, at startup, and a
        negative permanently skips shutdown monitoring for the life of the process. So one
        slow probe during instance boot must not silently disable interruption handling on a
        real EC2 host. Costs nothing where IMDS answers -- the first attempt returns. For what it costs where it
        does not, see TestImdsProbeCost.

        The permanence itself is not addressed here. _monitor_ec2_shutdown re-probes every
        poll and tolerates failure per iteration, so a monitor submitted unconditionally
        would self-correct; this gate exists so a non-EC2 host does not probe IMDS once a
        second forever. Making the verdict revisable is a behaviour change worth its own
        review rather than a rider on bounding the requests.
        """
        for attempt in range(1, Worker._IMDS_PROBE_ATTEMPTS + 1):
            if self._get_ec2_metadata_imdsv2_token():
                return True
            if attempt < Worker._IMDS_PROBE_ATTEMPTS:
                logger.debug(
                    "IMDS did not answer on attempt %d of %d; retrying",
                    attempt,
                    Worker._IMDS_PROBE_ATTEMPTS,
                )
                # Event.wait returns the flag, so this both sleeps and checks for shutdown.
                # Reporting not-EC2 on stop rather than spending the rest of the budget: the
                # monitor this gates would only be asked to stop.
                if self._stop.wait(timeout=Worker._IMDS_PROBE_BACKOFF_S):
                    logger.debug("Stop requested while probing IMDS; abandoning the probe")
                    return False
        logger.warning(
            "IMDS did not answer in %d attempts, so this host is treated as not being an "
            "EC2 instance. Spot interruption and ASG life-cycle changes will not be "
            "monitored for the life of this process.",
            Worker._IMDS_PROBE_ATTEMPTS,
        )
        return False

    def _get_ec2_metadata_imdsv2_token(self) -> str | None:
        """Query the EC2 Metadata service to obtain an IMDSv2 token to use in further queries to the
        service.

        Returns
        -------
        str | None
            None if we're not on EC2 or could not get a token from the metadata service. A token
            with a 10 second TTL otherwise.
        """
        # See:
        #  https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-service.html
        try:
            response = requests.put(
                "http://169.254.169.254/latest/api/token",
                headers={"X-aws-ec2-metadata-token-ttl-seconds": "10"},
                timeout=Worker._IMDS_REQUEST_TIMEOUT,
            )
        except requests.RequestException:
            # Could not connect to the metadata service. Either it's not enabled or we're not
            # on an EC2 instance.
            #
            #
            # The whole RequestException family rather than the (ConnectionError, Timeout)
            # pair the timeout raises, because "not on EC2" is the right answer for every
            # sibling -- a proxy that 302s the address, a truncated body -- and none of them
            # say anything else useful here. run() calls this inside its own try, so the
            # breadth is about giving the right answer rather than about containment.
            return None

        if response.status_code == 200:
            return response.text
        return None

    def _get_spot_instance_shutdown_action_timeout(self, *, imdsv2_token: str) -> timedelta | None:
        """Query the EC2 instance metadata service to check whether or not this instance is being
        stopped/terminated by the EC2 Spot service.

        Parameters
        ----------
        imdsv2_token : str
            An IMDSv2 token to authenticate the query.

        Returns
        -------
        timedelta | None
            None if we're not a Spot instance or if we have no pending EC2 Spot-driven shutdown.
            Otherwise, the time remaining before EC2 Spot is going to terminate the instance.
        """

        # See: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-instance-termination-notices.html#instance-action-metadata # noqa: E501
        try:
            response = requests.get(
                "http://169.254.169.254/latest/meta-data/spot/instance-action",
                headers={"X-aws-ec2-metadata-token": imdsv2_token},
                timeout=Worker._IMDS_REQUEST_TIMEOUT,
            )
        except requests.ReadTimeout:
            # ReadTimeout specifically, not Timeout: ConnectTimeout subclasses both
            # ConnectionError and Timeout, and it means the opposite of what this branch
            # reports -- see the clause below.
            #
            # Logged because None here is not "unknown" to the caller, it is "no interruption
            # pending", indistinguishable from a real answer. Without this a reachable-but-slow
            # IMDS reports "nothing is shutting down" every poll and a missed spot notice
            # leaves no trace of why.
            self._log_imds_unanswered("spot/instance-action", cause=Worker._IMDS_CAUSE_SLOW_READ)
            return None
        except requests.ConnectTimeout:
            # Reaching this method means the token request just succeeded, so the host is on
            # EC2 and IMDS was answering moments ago -- a connect that now times out is worth
            # surfacing rather than folding into the not-on-EC2 branch below, and it is not
            # "accepted the connection".
            self._log_imds_unanswered("spot/instance-action", cause=Worker._IMDS_CAUSE_NO_CONNECT)
            return None
        except requests.RequestException:
            # Could not connect to the metadata service. Either it's inactive or we're not
            # on an EC2 instance.
            return None

        # 404 is the answer on this endpoint when no interruption is pending, which is the
        # normal state of every healthy spot worker -- so it counts as answered. Only a status
        # that means nothing could be read leaves the path unanswered: getting bytes back is
        # not the same as getting an answer, and a 429 here produces the same silent "no
        # interruption pending" as a timeout does.
        if not _is_usable_imds_status(response.status_code):
            self._log_imds_unanswered(
                "spot/instance-action", cause=f"it returned HTTP {response.status_code}"
            )
            return None
        self._imds_answered("spot/instance-action")

        if response.status_code == 200:
            decoded_response = json.loads(response.text)
            if (action := decoded_response.get("action", None)) in ("stop", "terminate"):
                # We're getting shut down.
                if (shutdown_time := decoded_response.get("time", None)) is None:
                    # Should never happen. Being paranoid
                    logger.error(
                        "Missing 'time' property from ec2 metadata instance-action response"
                    )
                    return None
                logger.info(f"Spot {action} happening at {shutdown_time}")
                # Spot gives the time in UTC with a trailing Z, but Prior to Python 3.11 Python can't handle
                # the Z so we strip it
                shutdown_time = datetime.fromisoformat(shutdown_time[:-1])
                shutdown_time = shutdown_time.replace(tzinfo=timezone.utc)
                current_time = datetime.now(timezone.utc)
                time_delta = shutdown_time - current_time
                time_delta_seconds = int(time_delta.total_seconds())
                # Being paranoid. This will always be positive.
                if time_delta_seconds > 0:
                    return timedelta(seconds=time_delta_seconds)
                logger.error(f"Spot {action} time is in the past!")
        return None

    def _is_asg_terminated(self, *, imdsv2_token: str) -> bool:
        """Query the EC2 instance metadata service to determine whether an AutoscalingGroup has
        set this instance to transition to Terminated state.

        Parameters
        ----------
        imdsv2_token : str
            An IMDSv2 token to authenticate the query.

        Returns
        -------
        bool
            True if the instance is transitioning to Terminated; False otherwise.
        """
        # Return number of seconds until shutdown, if we're getting shut-down

        # See: https://docs.aws.amazon.com/autoscaling/ec2/userguide/retrieving-target-lifecycle-state-through-imds.html # noqa: E501
        try:
            response = requests.get(
                "http://169.254.169.254/latest/meta-data/autoscaling/target-lifecycle-state",
                headers={"X-aws-ec2-metadata-token": imdsv2_token},
                timeout=Worker._IMDS_REQUEST_TIMEOUT,
            )
        except requests.ReadTimeout:
            # Same reasoning as the spot query above: False here means "not terminating", so a
            # timeout that goes unlogged is indistinguishable from an answer.
            self._log_imds_unanswered(
                "autoscaling/target-lifecycle-state", cause=Worker._IMDS_CAUSE_SLOW_READ
            )
            return False
        except requests.ConnectTimeout:
            self._log_imds_unanswered(
                "autoscaling/target-lifecycle-state", cause=Worker._IMDS_CAUSE_NO_CONNECT
            )
            return False
        except requests.RequestException:
            return False

        # As with the spot endpoint, 404 is a real answer -- it is what a host outside an
        # auto-scaling group gets -- so only an unusable status counts as unanswered.
        if not _is_usable_imds_status(response.status_code):
            self._log_imds_unanswered(
                "autoscaling/target-lifecycle-state",
                cause=f"it returned HTTP {response.status_code}",
            )
            return False
        self._imds_answered("autoscaling/target-lifecycle-state")

        if response.status_code == 200:
            return response.text == "Terminated"
        return False

    def _log_imds_unanswered(self, path: str, *, cause: str) -> None:
        """Report an IMDS path that did not produce an answer the caller can act on.

        Warns on the transition into that state and stays quiet after, rather than logging on
        every poll: _monitor_ec2_shutdown runs once a second, so an unconditional warning would
        emit 3600 identical lines an hour while the condition persisted, which buries the very
        thing it is meant to make visible.

        Tracked per path rather than as one flag, because the monitor queries several paths per
        poll and any of them can fail while the others answer. A shared flag would be set by one
        and cleared by the next within a single iteration, producing both a warning and a
        spurious recovery line every second -- the outcome the suppression exists to prevent.

        `cause` is passed rather than derived because the reasons are not interchangeable: a read
        timeout means IMDS accepted the connection and did not finish, a connect timeout means
        the opposite, and a status like 429 means it answered with nothing usable. One message
        for all three would state the wrong cause for two of them.
        """
        if path in self._imds_unanswered:
            return
        self._imds_unanswered.add(path)
        logger.warning(
            "IMDS gave no usable answer for %s: %s. Treating it as no pending shutdown, so a "
            "spot interruption or ASG life-cycle change could be missed while this persists.",
            path,
            cause,
        )

    def _imds_answered(self, path: str) -> None:
        """Note that a path answered, so its next failure warns again.

        Only that path: another may still be unanswered, and claiming a recovery on its behalf
        is how an earlier shared-flag version reported IMDS coming back once a second.
        """
        if path in self._imds_unanswered:
            self._imds_unanswered.discard(path)
            logger.info("IMDS is answering %s again.", path)
