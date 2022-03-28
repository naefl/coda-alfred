import requests
from typing import Dict, List, Optional
import sys
from pathlib import Path
import os
from shutil import rmtree
import json
import pandas as pd
import click
from joblib import Memory
from datetime import date, timedelta

# this removes cache every day to invalidate
today = date.today()
yesterday = today - timedelta(1)
memory = Memory(f"/tmp/cachedir_{today.strftime('%Y-%m-%d')}", verbose=0)
old_cache=f"/tmp/cachedir_{yesterday.strftime('%Y-%m-%d')}"
if os.path.exists(old_cache):
    rmtree(old_cache)

@memory.cache
def alfred_list_docs():
    c = CodaClient()
    r  = c.list_docs(alfred=True)
    return {"items": r}

@memory.cache
def alfred_list_pages(pages:List[str]):
    c = CodaClient()
    r  = c.list_all_pages(pages, alfred=True)
    return {"items": r}

class CodaClient():
    # get current date
    def __init__(self):

        self.docs_url = "https://coda.io/apis/v1/docs"
        try:
            self.TOKEN = os.environ["CODA_TOKEN"]
            self.headers = {'Authorization': f'Bearer {self.TOKEN}'}
        except KeyError:
            print("Please set the CODA_TOKEN environment variable", sys.err)
            exit()


    def _auth_req(self,params:str=None, url:str=None) -> Dict[str,str]:
        kwargs = {"url":url,"params":params, "headers":self.headers}
        r = requests.get(**kwargs).json()
        token = r.get("nextPageToken", False)
        while token:
            kwargs["params"]["pageToken"] = token
            res = requests.get(**kwargs).json()
            r["items"].extend(res["items"])
            token = res.get("nextPageToken", False)
            link = res.get("nextPageLink", False)
        return r

    def _get_fields(self, r=Dict[str,str], fields: Optional[List[str]]=None, alfred:bool=False) -> List[Dict[str, str]]:
        resp: List[Dict[str,str]] = []
        for i in r:
            d: Dict[str,str] = {}
            for k, v in i.items():
                if k == "name" and alfred:
                    d["uid"] = v
                    d["title"] = v
                    d["subtitle"] = v
                    d["icon"] = "/Users/lucanaef/Downloads/coda.jpg"
                    continue
                if alfred:
                    d["variables"] = d.get("variables",{})
                    # setting environment variables in Alfred
                    if k in ["id", "browserLink"]:
                        d["variables"][k] = v
                        continue
                if k in fields:
                    d[k] = v
            resp.append(d)
        return resp

    def list_docs(self, id: bool = False, alfred: bool=False) -> Dict[str, str]:
        """
        Queries the Coda API for a doc with the given query
        """
        params = {
          'query': '',
        }
        r = self._auth_req(params=params,url=self.docs_url)["items"]
        fields = ["browserLink","name"]
        return self._get_fields(r, fields, alfred=alfred)

    def list_all_pages(self, pages: List[str], alfred: bool=False) -> Dict[str, str]:
        all: List[Dict[str,str]] = []
        for i in pages:
            r = self._auth_req(params={"limit":1000}, url=f"{self.docs_url}/{i}/pages/")["items"]
            fields = ["browserLink","name"]
            r = self._get_fields(r, fields, alfred)
            all.extend(r)
        return all

    def print_tables(self, doc:str, max_tables:int=10):
        uri = f"{self.docs_url}/{doc}/tables/"


        def get_cols(table):
            r = self._auth_req(params={}, url=f"{self.docs_url}/{doc}/tables/{table}/columns/")
            return [i["name"] for i in r["items"]]

        def get_rows(table):
            r = self._auth_req(params={}, url=f"{self.docs_url}/{doc}/tables/{table}/rows/")
            return [list(i["values"].values()) for i in r["items"]]

        resp = self._auth_req(url=uri, params={})

        k = 0
        for i in resp["items"]:
            if k > max_tables:
                continue
            idx = i["id"]
            rows = get_rows(idx)
            cols = get_cols(idx)
            print(pd.DataFrame(rows, columns=cols))
            k += 1


@click.command()
@click.option("-d", "--docs", default=None, help="list all docs", is_flag=True)
@click.option("-p", "--pages", default=None, help="list all docs")
@click.option("-a", "--alfred", default=None, help="format output compatible with alfred script filters", is_flag=True)
def main(docs, pages, alfred):
    if docs:
        if alfred:
            print(json.dumps(alfred_list_docs()))
    if pages:
        if alfred:
            print(json.dumps(alfred_list_pages([pages])))


if __name__ == "__main__":
    main()
