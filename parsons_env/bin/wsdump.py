#!/Users/cmarikos/pyspark/razea/actblue-pipeline/parsons_env/bin/python

import argparse
import code
import sys
import threading
import time
import ssl

import six
from six.moves.urllib.parse import urlparse

import websocket

try:
    import readline
except ImportError:
    pass


def get_encoding():
    encoding = getattr(sys.stdin, "encoding", "")
    if not encoding:
        return "utf-8"
    else:
        return encoding.lower()


OPCODE_DATA = (websocket.ABNF.OPCODE_TEXT, websocket.ABNF.OPCODE_BINARY)
ENCODING = get_encoding()


class VAction(argparse.Action):

    def __call__(self, parser, args, values, option_string=None):
        if values is None:
            values = "1"
        try:
            values = int(values)
        except ValueError:
            values = values.count("v") + 1
        setattr(args, self.dest, values)


def parse_args():
    parser = argparse.ArgumentParser(description="WebSocket Simple Dump Tool")
    parser.add_argument("url", metavar="ws_url",
                        help="websocket url. ex. ws://echo.websocket.org/")
    parser.add_argument("-p", "--proxy",
                        help="proxy url. ex. http://127.0.0.1:8080")
    parser.add_argument("-v", "--verbose", default=0, nargs='?', action=VAction,
                        dest="verbose",
                        help="set verbose mode. If set to 1, show opcode. "
                        "If set to 2, enable to trace  websocket module")
    parser.add_argument("-n", "--nocert", action='store_true',
                        help="Ignore invalid SSL cert")
    parser.add_argument("-r", "--raw", action="store_true",
                        help="raw output")
    parser.add_argument("-s", "--subprotocols", nargs='*',
                        help="Set subprotocols")
    parser.add_argument("-o", "--origin",
                        help="Set origin")
    parser.add_argument("--eof-wait", default=0, type=int,
                        help="wait time(second) after 'EOF' received.")
    parser.add_argument("-t", "--text",
                        help="Send initial text")
    parser.add_argument("--timings", action="store_true",
                        help="Print timings in seconds")
    parser.add_argument("--headers",
                        help="Set custom headers. Use ',' as separator")

    return parser.parse_args()


class RawInput:

    def raw_input(self, prompt):
        if six.PY3:
            line = input(prompt)
        else:
            line = raw_input(prompt)

        if ENCODING and ENCODING != "utf-8" and not isinstance(line, six.text_type):
            line = line.decode(ENCODING).encode("utf-8")
        elif isinstance(line, six.text_type):
            line = line.encode("utf-8")

        return line


class InteractiveConsole(RawInput, code.InteractiveConsole):

    def write(self, data):
        sys.stdout.write("\033[2K\033[E")
        # sys.stdout.write("\n")
        sys.stdout.write("\033[34m< " + data + "\033[39m")
        sys.stdout.write("\n> ")
        sys.stdout.flush()

    def read(self):
        return self.raw_input("> ")


class NonInteractive(RawInput):

    def write(self, data):
        sys.stdout.write(data)
        sys.stdout.write("\n")
        sys.stdout.flush()

    def read(self):
        return self.raw_input("")


def main():
    start_time = time.time()
    args = parse_args()
    if args.verbose > 1:
        websocket.enableTrace(True)
    options = {}
    if args.proxy:
        p = urlparse(args.proxy)
        options["http_proxy_host"] = p.hostname
        options["http_proxy_port"] = p.port
    if args.origin:
        options["origin"] = args.origin
    if args.subprotocols:
        options["subprotocols"] = args.subprotocols
    opts = {}
    if args.nocert:
        opts = {"cert_reqs": ssl.CERT_NONE, "check_hostname": False}
    if args.headers:
        options['header'] = map(str.strip, args.headers.split(','))
    ws = websocket.create_connection(args.url, sslopt=opts, **options)
    if args.raw:
        console = NonInteractive()
    else:
        console = InteractiveConsole()
        print("Press Ctrl+C to quit")

    def recv():
        try:
            frame = ws.recv_frame()
        except websocket.WebSocketException:
            return websocket.ABNF.OPCODE_CLOSE, None
        if not frame:
            raise websocket.WebSocketException("Not a valid frame %s" % frame)
        elif frame.opcode in OPCODE_DATA:
            return frame.opcode, frame.data
        elif frame.opcode == websocket.ABNF.OPCODE_CLOSE:
            ws.send_close()
            return frame.opcode, None
        elif frame.opcode == websocket.ABNF.OPCODE_PING:
            ws.pong(frame.data)
            return frame.opcode, frame.data

        return frame.opcode, frame.data

    def recv_ws():
        while True:
            opcode, data = recv()
            msg = None
            if six.PY3 and opcode == websocket.ABNF.OPCODE_TEXT and isinstance(data, bytes):
                data = str(data, "utf-8")
            if not args.verbose and opcode in OPCODE_DATA:
                msg = data
            elif args.verbose:
                msg = "%s: %s" % (websocket.ABNF.OPCODE_MAP.get(opcode), data)

            if msg is not None:
                if args.timings:
                    console.write(str(time.time() - start_time) + ": " + msg)
                else:
                    console.write(msg)

            if opcode == websocket.ABNF.OPCODE_CLOSE:
                break

    thread = threading.Thread(target=recv_ws)
    thread.daemon = True
    thread.start()

    if args.text:
        ws.send(args.text)

    while True:
        try:
            message = console.read()
            ws.send(message)
        except KeyboardInterrupt:
            return
        except EOFError:
            time.sleep(args.eof_wait)
            return


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e)
