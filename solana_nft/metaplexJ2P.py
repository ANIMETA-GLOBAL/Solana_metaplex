import redis
import config
import uuid
import time
import json
# pool.publish("solana", "test")
from pprint import pprint as pp

class RequestStruct(object):

    def __init__(self, function_name: str, args: dict):
        self.id = uuid.uuid1().int

        self.function_name = function_name

        self.args = args

    def request(self):
        rq = {
            "id": str(self.id),
            "function": self.function_name,
            "args": self.args,
            "gen_ts": time.time()
        }
        return json.dumps(rq)


class MetaPlexClient(object):

    def __init__(self):
        self.pool = redis.Redis(host=config.redis_host, port=config.redis_port, decode_responses=True,
                                password=config.redis_pwd,
                                db=0)

        self.pub_ch = "rq_solana"
        self.sub_ch = "rp_solana"

    def publish(self, request):
        self.pool.publish(self.pub_ch, request)

    def response_from_id(self, id,timeout = 60):
        time_count = 0
        get_response = False
        while time_count < (timeout/0.5):
            time_count += 1
            time.sleep(0.5)
            res = self.pool.get(id)
            print(time_count)
            if res and not get_response:
                get_response = True
                res = None
            if res and get_response and "gen_ts" not in res:
                return res

    def get_all_by_owner(self, address):
        req = RequestStruct("getAllByOwner", {"address": address})
        self.publish(req.request())
        result = self.response_from_id(req.id)
        # print(result)
        return result

    def create_nft(self, uri:str,name:str,fee:int):
        req = RequestStruct("createNFT", {"uri": uri,"name":name,"fee":fee})
        self.publish(req.request())
        result = self.response_from_id(req.id)
        # print(result)
        return result



if __name__ == '__main__':
    A = MetaPlexClient()
    res = A.get_all_by_owner(address="GpjmSMc9mUcwuTcKoHyuiTZ9vjEq8QAqH3Y7mexXQUo")
    # res = A.create_nft(uri="https://ipfs.io/ipfs/QmWtsYsCt5sWCqC6B5fqWeDVmJTBCShy4fo5GXNFCKvweQ/0",name="Cool",fee=0)

    pp(json.loads(res))
