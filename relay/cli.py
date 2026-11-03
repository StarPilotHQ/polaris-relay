"""`polaris-relay serve`: the web UI and the worker in one process."""
import argparse
import logging
import os
import sys

from relay import __version__


def main(argv: list[str] | None = None) -> int:
  p = argparse.ArgumentParser(prog="polaris-relay")
  sub = p.add_subparsers(dest="cmd", required=True)
  serve = sub.add_parser("serve", help="run the web UI and the worker")
  serve.add_argument("--host", default="0.0.0.0")
  serve.add_argument("--port", type=int, default=8080)
  sub.add_parser("version")
  args = p.parse_args(argv)
  os.umask(0o077)  # the database, working files and session key are the user's alone
  logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(name)s %(message)s")
  logging.getLogger("apprise").setLevel(logging.WARNING)  # it logs target URLs, which carry webhook secrets
  log = logging.getLogger("relay")
  if args.cmd == "version":
    print(__version__)
    return 0

  from relay.config import Config, StartupError
  try:
    config = Config.from_env()
  except StartupError as e:
    log.error("not starting: %s", e)
    return 2
  for w in config.warnings:
    log.warning("%s", w)

  import uvicorn

  from relay.db import make_sessionmaker
  from relay.web.app import create_app
  app = create_app(config, make_sessionmaker(config.data_dir), start_worker=True)
  log.info("Polaris Relay %s listening on %s:%d", __version__, args.host, args.port)
  # The access log would record every polled page; it carries nothing useful and route links pass through it.
  uvicorn.run(app, host=args.host, port=args.port, access_log=False, proxy_headers=False)
  return 0


if __name__ == "__main__":
  sys.exit(main())
