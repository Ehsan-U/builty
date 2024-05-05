from itertools import chain
import json
import random
import re
import string
from typing import Dict, Iterable, Tuple
from urllib.parse import urlencode
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.http import Response
from scrapy.utils.defer import maybe_deferred_to_future



class Marionfl(scrapy.Spider):
    name = "marionfl_spider"


    def start_requests(self) -> Iterable[scrapy.Request]:
        """
        Entry point, Generates the initial requests to the website.
        """
        url = "https://cdplusmobile.marioncountyfl.org/pdswebservices/PROD/webpermitnew/webpermits.dll"
        with open("permits.json", 'r') as f:
            permits = list(set(chain.from_iterable(item.values() for item in json.load(f))))
        for permit in permits[:105]:
            yield scrapy.Request(url, callback=self.parse, cb_kwargs={"permit": permit}, dont_filter=True)

    
    async def parse(self, response: Response, permit: str) -> Dict:
        session_id, window_id = response.xpath("//input[@name='IW_SessionID_']/@value").get(), response.xpath("//input[@name='IW_WindowID_']/@value").get()
        # submit session form
        await self.register_session(session_id, window_id)
        
        # get permit iframe
        ajax_id = self.get_ajax_id()
        await self.set_timer(session_id, ajax_id)
        await self.click_permit_btn(session_id, ajax_id)
        await self.set_trackid(session_id)
        await self.set_timer(session_id, ajax_id, timer_type="main") # trackid updated to 5

        # submit permit
        ajax_id = self.get_ajax_id()
        trackid = await self.submit_permit(session_id, ajax_id, permit)
        if trackid is None:
            self.logger.info(f"Permit does not exist! {permit}")
            return None

        # get data from available tabs
        trackid, detail_item, tabs_status = await self.get_detail_tab(session_id, ajax_id, permit, trackid, callback=self.parse_detail_tab) 
        trackid, inspection_item = await self.get_inspection_tab(session_id, ajax_id, trackid, callback=self.parse_tab) if tabs_status.get('inspection') else (trackid, [])
        trackid, review_item = await self.get_review_tab(session_id, ajax_id, trackid, callback=self.parse_tab) if tabs_status.get('review') else (trackid, [])
        trackid, permit_holds_item = await self.get_permit_holds_tab(session_id, ajax_id, trackid, callback=self.parse_tab) if tabs_status.get('permit_hold') else (trackid, [])
        trackid, fees_item = await self.get_fees_tab(session_id, ajax_id, trackid, callback=self.parse_tab) if tabs_status.get('fees') else (trackid, [])
        trackid, subs_item = await self.get_subs_tab(session_id, ajax_id, trackid, callback=self.parse_tab) if tabs_status.get('subs') else (trackid, [])
        trackid, cos_item = await self.get_cos_tab(session_id, ajax_id, trackid, callback=self.parse_tab) if tabs_status.get('cos') else (trackid, [])

        item = dict(
            permit=permit,
            detail=detail_item,
            inspection=inspection_item,
            reviews=review_item,
            permit_holds=permit_holds_item,
            fees=fees_item,
            subs=subs_item,
            cos=cos_item
        )
        return item
    

    async def register_session(self, session_id: str, window_id: str) -> None:
        url = f"https://cdplusmobile.marioncountyfl.org/pdswebservices/PROD/webpermitnew/webpermits.dll/{session_id}/"
        data = {
            'IW_width': '728',
            'IW_height': '797',
            'IW_dpr': '1',
            'IW_SessionID_': session_id,
            'IW_TrackID_': '1',
            'IW_WindowID_': window_id,
        }
        d = self.crawler.engine.download(scrapy.FormRequest(url, formdata=data))
        response = await maybe_deferred_to_future(d)
        return


    async def set_timer(self, session_id: str, ajax_id: str, timer_type: str = None) -> None:
        params = {
            'callback': 'TIMERLOAD.DoOnAsyncTimer',
            'IW_WindowID_': 'I1',
            'IW_TrackID_': '1',
            'IW_SessionID_': session_id,
            'IW_FormClass': 'TFrmStart' if timer_type is None else "TFrmMain",
            'IW_FormName': 'FrmStart' if timer_type is None else "FrmMain",
            'IW_AjaxID': ajax_id,
        }
        url = f"https://cdplusmobile.marioncountyfl.org/pdswebservices/PROD/webpermitnew/webpermits.dll/{session_id}/$/callback?"+urlencode(params)
        d = self.crawler.engine.download(scrapy.Request(url=url))
        response = await maybe_deferred_to_future(d)
        return


    async def click_permit_btn(self, session_id: str, ajax_id: str) -> None:
        url = f"https://cdplusmobile.marioncountyfl.org/pdswebservices/PROD/webpermitnew/webpermits.dll/{session_id}/$/callback?callback=BTNPERMITS.DoOnAsyncClick&x=161&y=23&which=0&modifiers="
        data = {
            'BTNPERMITS': '',
            'IW_FormName': 'FrmStart',
            'IW_FormClass': 'TFrmStart',
            'IW_width': '728',
            'IW_height': '797',
            'IW_Action': 'BTNPERMITS',
            'IW_ActionParam': '',
            'IW_Offset': '',
            'IW_SessionID_': session_id,
            'IW_TrackID_': '2',
            'IW_WindowID_': 'I1',
            'IW_AjaxID': ajax_id,
        }
        d = self.crawler.engine.download(scrapy.FormRequest(url, formdata=data))
        response = await maybe_deferred_to_future(d)
        return


    async def set_trackid(self, session_id: str) -> None:
        url = f"https://cdplusmobile.marioncountyfl.org/pdswebservices/PROD/webpermitnew/webpermits.dll/{session_id}/"
        data = {
            'IW_SessionID_': session_id,
            'IW_TrackID_': '3',
        }
        d = self.crawler.engine.download(scrapy.FormRequest(url, formdata=data))
        response = await maybe_deferred_to_future(d)
        return 


    async def submit_permit(self, session_id: str, ajax_id: str, permit: str) -> str:
        url = f"https://cdplusmobile.marioncountyfl.org/pdswebservices/PROD/webpermitnew/webpermits.dll/{session_id}/$/callback?callback=EDTPERMITNBR.DoOnAsyncKeyUp&which=0&modifiers="
        data = {
            'EDTPERMITNBR': permit,
            'IW_FormName': 'FrmMain',
            'IW_FormClass': 'TFrmMain',
            'IW_width': '728',
            'IW_height': '797',
            'IW_Action': 'EDTPERMITNBR',
            'IW_ActionParam': '',
            'IW_Offset': '',
            'IW_SessionID_': session_id,
            'IW_TrackID_': '5',
            'IW_WindowID_': 'I1',
            'IW_AjaxID': ajax_id,
        }
        d = self.crawler.engine.download(scrapy.FormRequest(url, formdata=data))
        response = await maybe_deferred_to_future(d)
        if 'no matching permit' in response.text.lower():
            return None
        return response.xpath("//trackid/text()").get()
    

    async def get_tab(self, session_id: str, trackid: str, callback=callable, return_iframe: bool = False) -> Dict | Tuple[Dict, Response]:
        url = f"https://cdplusmobile.marioncountyfl.org/pdswebservices/PROD/webpermitnew/webpermits.dll/{session_id}/"
        data = {
            'IW_SessionID_': session_id,
            'IW_TrackID_': trackid,
        }
        d = self.crawler.engine.download(scrapy.FormRequest(url, formdata=data))
        response = await maybe_deferred_to_future(d)
        if return_iframe:
            return callback(response), response
        return callback(response)
    

    async def go_back(self, session_id: str, ajax_id: str, trackid: str, formname: str, formclass: str):
        url = f"https://cdplusmobile.marioncountyfl.org/pdswebservices/PROD/webpermitnew/webpermits.dll/{session_id}/$/callback?callback=IMGBACK.DoOnAsyncClick&x=46&y=21&which=0&modifiers="
        data = {
            'IW_FormName': formname,
            'IW_FormClass': formclass,
            'IW_width': '728',
            'IW_height': '797',
            'IW_Action': 'IMGBACK',
            'IW_ActionParam': '',
            'IW_Offset': '',
            'IW_SessionID_': session_id,
            'IW_TrackID_': trackid,
            'IW_WindowID_': 'I1',
            'IW_AjaxID': ajax_id,
        }
        d = self.crawler.engine.download(scrapy.FormRequest(url, formdata=data))
        response = await maybe_deferred_to_future(d)
        trackid = response.xpath("//trackid/text()").get() or response.xpath("//*").re_first(r'"IW_TrackID_": (\d+)')
        return trackid


    async def get_detail_tab(self, session_id: str, ajax_id: str, permit: str, trackid: str, callback: callable) -> Tuple[str, Dict, Dict]:
        url = f"https://cdplusmobile.marioncountyfl.org/pdswebservices/PROD/webpermitnew/webpermits.dll/{session_id}/$/callback?callback=BTNGUESTLOGIN.DoOnAsyncClick&x=118&y=29&which=0&modifiers="
        data = {
            'EDTPERMITNBR': permit,
            'BTNGUESTLOGIN': '',
            'IW_FormName': 'FrmMain',
            'IW_FormClass': 'TFrmMain',
            'IW_width': '728',
            'IW_height': '797',
            'IW_Action': 'BTNGUESTLOGIN',
            'IW_ActionParam': '',
            'IW_Offset': '',
            'IW_SessionID_': session_id,
            'IW_TrackID_': trackid,
            'IW_WindowID_': 'I1',
            'IW_AjaxID': ajax_id,
        }
        d = self.crawler.engine.download(scrapy.FormRequest(url, formdata=data))
        response = await maybe_deferred_to_future(d)
        trackid = response.xpath("//trackid/text()").get() or response.xpath("//*").re_first(r'"IW_TrackID_": (\d+)')
        item, iframe = await self.get_tab(session_id, trackid, callback=callback, return_iframe=True)
        other_tabs = self.get_tabs_status(iframe)
        return trackid, item, other_tabs


    async def get_inspection_tab(self, session_id: str, ajax_id: str, trackid: str, callback=callable) -> Tuple[str, Dict]:
        url = f"https://cdplusmobile.marioncountyfl.org/pdswebservices/PROD/webpermitnew/webpermits.dll/{session_id}/$/callback?callback=BTNVIEWINSPECTIONS.DoOnAsyncClick&x=42&y=14&which=0&modifiers="
        data = {
            'BTNVIEWINSPECTIONS': '',
            'IW_FormName': 'FrmPermitDetail',
            'IW_FormClass': 'TFrmPermitDetail',
            'IW_width': '728',
            'IW_height': '797',
            'IW_Action': 'BTNVIEWINSPECTIONS',
            'IW_ActionParam': '',
            'IW_Offset': '',
            'IW_SessionID_': session_id,
            'IW_TrackID_': trackid,
            'IW_WindowID_': 'I1',
            'IW_AjaxID': ajax_id,
        }
        d = self.crawler.engine.download(scrapy.FormRequest(url, formdata=data))
        response = await maybe_deferred_to_future(d)
        trackid = response.xpath("//trackid/text()").get() or response.xpath("//*").re_first(r'"IW_TrackID_": (\d+)')
        item = await self.get_tab(session_id, trackid, callback=callback)
        trackid = await self.go_back(session_id, ajax_id, trackid, "FrmPermitInspections", "TFrmPermitInspections")
        return trackid, item
    

    async def get_review_tab(self, session_id: str, ajax_id: str, trackid: str, callback=callable) -> Tuple[str, Dict]:
        url = f"https://cdplusmobile.marioncountyfl.org/pdswebservices/PROD/webpermitnew/webpermits.dll/{session_id}/$/callback?callback=BTNVIEWPLANREVIEWS.DoOnAsyncClick&x=36&y=19&which=0&modifiers="
        data = {
            'BTNVIEWPLANREVIEWS': '',
            'IW_FormName': 'FrmPermitDetail',
            'IW_FormClass': 'TFrmPermitDetail',
            'IW_width': '728',
            'IW_height': '797',
            'IW_Action': 'BTNVIEWPLANREVIEWS',
            'IW_ActionParam': '',
            'IW_Offset': '',
            'IW_SessionID_': session_id,
            'IW_TrackID_': trackid,
            'IW_WindowID_': 'I1',
            'IW_AjaxID': ajax_id,
        }
        d = self.crawler.engine.download(scrapy.FormRequest(url, formdata=data))
        response = await maybe_deferred_to_future(d)
        trackid = response.xpath("//trackid/text()").get() or response.xpath("//*").re_first(r'"IW_TrackID_": (\d+)')
        item = await self.get_tab(session_id, trackid, callback=callback)
        trackid = await self.go_back(session_id, ajax_id, trackid, "FrmPlanReviews", "TFrmPlanReviews")
        return trackid, item


    async def get_permit_holds_tab(self, session_id: str, ajax_id: str, trackid: str, callback=callable) -> Tuple[str, Dict]:
        url = f"https://cdplusmobile.marioncountyfl.org/pdswebservices/PROD/webpermitnew/webpermits.dll/{session_id}/$/callback?callback=BTNPERMITHOLDS.DoOnAsyncClick&x=58&y=17&which=0&modifiers="
        data = {
            'BTNPERMITHOLDS': '',
            'IW_FormName': 'FrmPermitDetail',
            'IW_FormClass': 'TFrmPermitDetail',
            'IW_width': '728',
            'IW_height': '797',
            'IW_Action': 'BTNPERMITHOLDS',
            'IW_ActionParam': '',
            'IW_Offset': '',
            'IW_SessionID_': session_id,
            'IW_TrackID_': trackid,
            'IW_WindowID_': 'I1',
            'IW_AjaxID': ajax_id,
        }
        d = self.crawler.engine.download(scrapy.FormRequest(url, formdata=data))
        response = await maybe_deferred_to_future(d)
        trackid = response.xpath("//trackid/text()").get() or response.xpath("//*").re_first(r'"IW_TrackID_": (\d+)')
        item = await self.get_tab(session_id, trackid, callback=callback)
        trackid = await self.go_back(session_id, ajax_id, trackid, "FrmComments", "TFrmComments")
        return trackid, item


    async def get_fees_tab(self, session_id: str, ajax_id: str, trackid: str, callback=callable) -> Tuple[str, Dict]:
        url = f"https://cdplusmobile.marioncountyfl.org/pdswebservices/PROD/webpermitnew/webpermits.dll/{session_id}/$/callback?callback=BTNVIEWFEES.DoOnAsyncClick&x=60&y=19&which=0&modifiers="
        data = {
            'BTNVIEWFEES': '',
            'IW_FormName': 'FrmPermitDetail',
            'IW_FormClass': 'TFrmPermitDetail',
            'IW_width': '728',
            'IW_height': '797',
            'IW_Action': 'BTNVIEWFEES',
            'IW_ActionParam': '',
            'IW_Offset': '',
            'IW_SessionID_': session_id,
            'IW_TrackID_': trackid,
            'IW_WindowID_': 'I1',
            'IW_AjaxID': ajax_id,
        }
        d = self.crawler.engine.download(scrapy.FormRequest(url, formdata=data))
        response = await maybe_deferred_to_future(d)
        trackid = response.xpath("//trackid/text()").get() or response.xpath("//*").re_first(r'"IW_TrackID_": (\d+)')
        item = await self.get_tab(session_id, trackid, callback=callback)
        trackid = await self.go_back(session_id, ajax_id, trackid, "FrmFees", "TFrmFees")
        return trackid, item
    

    async def get_subs_tab(self, session_id: str, ajax_id: str, trackid: str, callback=callable) -> Tuple[str, Dict]:
        url = f"https://cdplusmobile.marioncountyfl.org/pdswebservices/PROD/webpermitnew/webpermits.dll/{session_id}/$/callback?callback=BTNSUBS.DoOnAsyncClick&x=26&y=22&which=0&modifiers="
        data = {
            'BTNSUBS': '',
            'IW_FormName': 'FrmPermitDetail',
            'IW_FormClass': 'TFrmPermitDetail',
            'IW_width': '728',
            'IW_height': '797',
            'IW_Action': 'BTNSUBS',
            'IW_ActionParam': '',
            'IW_Offset': '',
            'IW_SessionID_': session_id,
            'IW_TrackID_': trackid,
            'IW_WindowID_': 'I1',
            'IW_AjaxID': ajax_id,
        }
        d = self.crawler.engine.download(scrapy.FormRequest(url, formdata=data))
        response = await maybe_deferred_to_future(d)
        trackid = response.xpath("//trackid/text()").get() or response.xpath("//*").re_first(r'"IW_TrackID_": (\d+)')
        item = await self.get_tab(session_id, trackid, callback=callback)
        trackid = await self.go_back(session_id, ajax_id, trackid, "FrmSubContractors", "TFrmSubContractors")
        return trackid, item


    async def get_cos_tab(self, session_id: str, ajax_id: str, trackid: str, callback=callable) -> Tuple[str, Dict]:
        url = f"https://cdplusmobile.marioncountyfl.org/pdswebservices/PROD/webpermitnew/webpermits.dll/{session_id}/$/callback?callback=BTNVIEWCOS.DoOnAsyncClick&x=24&y=16&which=0&modifiers="
        data = {
            'BTNVIEWCOS': '',
            'IW_FormName': 'FrmPermitDetail',
            'IW_FormClass': 'TFrmPermitDetail',
            'IW_width': '728',
            'IW_height': '797',
            'IW_Action': 'BTNVIEWCOS',
            'IW_ActionParam': '',
            'IW_Offset': '',
            'IW_SessionID_': session_id,
            'IW_TrackID_': trackid,
            'IW_WindowID_': 'I1',
            'IW_AjaxID': ajax_id,
        }
        d = self.crawler.engine.download(scrapy.FormRequest(url, formdata=data))
        response = await maybe_deferred_to_future(d)
        trackid = response.xpath("//trackid/text()").get() or response.xpath("//*").re_first(r'"IW_TrackID_": (\d+)')
        item = await self.get_tab(session_id, trackid, callback=callback)
        trackid = await self.go_back(session_id, ajax_id, trackid, "FrmCertOcc", "TFrmCertOcc")
        return trackid, item


    @staticmethod
    def parse_detail_tab(response: Response) -> Dict:
        item = dict(
            permit_status=response.xpath("//input[@id='BTNPRINTJOBCARD']/parent::form/input[@id='IWDBEDIT2']/@value").get(),
            type=response.xpath("//input[@id='BTNPRINTJOBCARD']/parent::form/input[@id='IWDBEDIT12']/@value").get('') + ', ' + response.xpath("//input[@id='BTNPRINTJOBCARD']/parent::form/input[@id='IWDBEDIT3']/@value").get(''),
            owner=response.xpath("//input[@id='BTNPRINTJOBCARD']/parent::form/input[@id='IWDBEDIT4']/@value").get(),
            address=response.xpath("//input[@id='BTNPRINTJOBCARD']/parent::form/input[@id='IWDBEDIT5']/@value").get(),
            parcel=response.xpath("//input[@id='BTNPRINTJOBCARD']/parent::form/input[@id='IWDBEDIT14']/@value").get(),
            dba=response.xpath("//input[@id='BTNPRINTJOBCARD']/parent::form/input[@id='IWDBEDIT6']/@value").get(),
            job_desc=response.xpath("//input[@id='BTNPRINTJOBCARD']/parent::form/input[@id='IWDBMEMO1'] | //input[@id='BTNPRINTJOBCARD']/parent::form/textarea/text()").get(),
            apply_date=response.xpath("//input[@id='BTNPRINTJOBCARD']/parent::form/input[@id='IWDBEDIT13']/@value").get(),
            issued_date=response.xpath("//input[@id='BTNPRINTJOBCARD']/parent::form/input[@id='IWDBEDIT8']/@value").get(),
            co_date=response.xpath("//input[@id='BTNPRINTJOBCARD']/parent::form/input[@id='IWDBEDIT7']/@value").get(),
            expiration_date=response.xpath("//input[@id='BTNPRINTJOBCARD']/parent::form/input[@id='IWDBEDIT9']/@value").get(),
            last_inspection_request=response.xpath("//input[@id='BTNPRINTJOBCARD']/parent::form/input[@id='IWDBEDIT10']/@value").get(),
            last_inspection_result=response.xpath("//input[@id='BTNPRINTJOBCARD']/parent::form/input[@id='IWDBEDIT11']/@value").get(),
        )
        return {k:v.strip() if isinstance(v, str) else v for k,v in item.items()}


    @staticmethod
    def parse_tab(response: Response) -> Dict:
        headers = response.xpath("//td[@onclick]//table[contains(@id,'GRID_')]/tr[1]/td//b/span/text()").getall()
        item = []
        for row in response.xpath("//td[@onclick]//table[contains(@id,'GRID_')]/tr")[1:]:
            row_item = {}
            for header, value in zip(headers, row.xpath("./td//div/text()").getall()):
                if value.strip():
                    row_item[header] = value
            if row_item:
                row_item = {k:v.strip() if isinstance(v, str) else v for k,v in row_item.items()}
                item.append(row_item)
        return item


    @staticmethod
    def get_tabs_status(response: Response) -> Dict:
        statuses = re.findall("\.attr\('data-badge','(\d+)'\)", response.xpath("//script[@nonce][2]").get(''))
        if statuses and len(statuses) == 6:
            return {
                "review": int(statuses[0] or 0),
                "fees": int(statuses[1] or 0),
                "inspection": int(statuses[2] or 0),
                "subs": int(statuses[3] or 0),
                "cos": int(statuses[4] or 0),
                "permit_hold": int(statuses[5] or 0),
            }
        return {}
    

    @staticmethod
    def get_ajax_id() -> str:
        sample_string = "17148202878785"
        first_6_digits = sample_string[:11]
        remaining_digits = sample_string[11:]
        new_remaining_digits = ''.join(random.choice(string.digits) for _ in range(len(remaining_digits)))
        return first_6_digits + new_remaining_digits
    



########################################


crawler = CrawlerProcess(settings=dict(
    CONCURRENT_REQUESTS=4,
    TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    FEEDS={"sample.json": {"format": "json"}},
    COOKIES_ENABLED=False,
    DEFAULT_REQUEST_HEADERS={
        'Host': 'cdplusmobile.marioncountyfl.org',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'Accept': '*/*',
        'Referer': 'https://cdplusmobile.marioncountyfl.org/pdswebservices/PROD/webpermitnew/webpermits.dll',
        'Accept-Language': 'en-US,en;q=0.9,ur;q=0.8,af;q=0.7',
    }
))
crawler.crawl(Marionfl)
crawler.start()