import json
import sys
from pathlib import Path
from traceback import format_exc

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from google_product_promo.mcp_tools import call_tool, list_tools


def _write_response(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()


def handle_request(request):
    method = request.get("method")
    request_id = request.get("id")
    if method == "tools/list":
        return {"id": request_id, "result": {"tools": list_tools()}}
    if method == "tools/call":
        params = request.get("params") or {}
        name = params.get("name")
        arguments = params.get("arguments") or {}
        return {"id": request_id, "result": call_tool(name, arguments)}
    return {
        "id": request_id,
        "error": {
            "code": -32601,
            "message": f"Unsupported method: {method}",
        },
    }


def main():
    for raw_line in sys.stdin:
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        try:
            request = json.loads(raw_line)
            response = handle_request(request)
        except Exception as exc:
            response = {
                "id": None,
                "error": {
                    "code": -32000,
                    "message": str(exc),
                    "details": format_exc(),
                },
            }
        _write_response(response)


if __name__ == "__main__":
    main()
