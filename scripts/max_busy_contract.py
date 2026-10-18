"""Native busy-handler contract: consumption is neither rejection nor a new turn."""

import argparse
import asyncio
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile


async def exercise(plugin_dir,home):
    os.environ["HERMES_HOME"]=str(home)
    from gateway.config import PlatformConfig
    from gateway.platform_registry import PlatformEntry,platform_registry
    from gateway.platforms.base import BasePlatformAdapter
    spec=importlib.util.spec_from_file_location("max_busy_contract_plugin",plugin_dir/"__init__.py",submodule_search_locations=[str(plugin_dir)])
    module=importlib.util.module_from_spec(spec)
    sys.modules[spec.name]=module
    spec.loader.exec_module(module)
    class Context:
        def register_platform(self,**kwargs): platform_registry.register(PlatformEntry(**kwargs))
    module.register(Context())
    adapter=module.MaxAdapter(PlatformConfig(enabled=True,token="synthetic",extra={"allow_from":["42"]}))
    assert isinstance(adapter,BasePlatformAdapter)
    async def message_handler(event): raise AssertionError("Must use busy path, not model")
    adapter.set_message_handler(message_handler)
    async def cached(message,event): pass
    adapter._download_media_for_chat=cached
    update={"update_type":"message_created","message":{"sender":{"user_id":42},"recipient":{"chat_type":"dialog"},
        "body":{"mid":"voice-busy","attachments":[{"type":"audio","payload":{"url":"https://a.oneme.ru/test"}}]}}}
    message=module.adapter.MaxMessage.from_update(update)
    event=module.adapter._build_message_event(adapter,message)
    key=adapter._event_session_key(event)
    hold=asyncio.Event()
    task=asyncio.create_task(hold.wait())
    adapter._active_sessions[key]=asyncio.Event()
    adapter._session_tasks[key]=task
    try:
        for admitted,accepted,expected in ((True,False,"consumed"),(False,False,"rejected"),(True,True,"accepted")):
            async def busy(event,session_key):
                event._bot_loop_admitted=admitted
                event._gateway_accepted=accepted
                return True
            adapter.set_busy_session_handler(busy)
            assert await adapter._dispatch_update(update)==expected
        return {"native_base":BasePlatformAdapter.__module__,"consumed_distinct_from_accepted":True,
                "unauthorized_not_success":True,"model_started":False,"network":False}
    finally:
        task.cancel()
        try: await task
        except asyncio.CancelledError: pass


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--hermes-root",required=True,type=Path)
    parser.add_argument("--plugin-dir",type=Path,default=Path(__file__).resolve().parents[1]/"plugins/max")
    args=parser.parse_args()
    if not (args.hermes_root/"gateway/platforms/base.py").is_file(): raise SystemExit("Настоящий Hermes обязателен")
    sys.path.insert(0,str(args.hermes_root.resolve()))
    def audit(event,_args):
        if event in {"socket.connect","socket.getaddrinfo","socket.bind"}: raise RuntimeError("Network forbidden")
    sys.addaudithook(audit)
    with tempfile.TemporaryDirectory(prefix="max-busy-contract-") as home:
        result=asyncio.run(exercise(args.plugin_dir.resolve(),Path(home)))
    print(json.dumps(result))


if __name__=="__main__": main()
