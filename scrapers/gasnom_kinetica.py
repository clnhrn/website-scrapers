import uuid
from datetime import date, timedelta
from io import StringIO
import logging


import pandas


from scrapers import PipelineScraper

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class GasNom(PipelineScraper):
    source = 'gasnom-kinetica'
    api_url = 'http://www.gasnom.com/ip/kinetica/'
    post_data_url = 'http://www.gasnom.com/ip/kinetica/OAC.cfm'
    current_post_date = date.today().strftime('%m/%d/%Y')

    post_page_headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'max-age=0',
        'Connection': "keep-alive",
        'Content-Length': '71',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cookie': 'visid_incap_2551=OEm7s+EsTk2fDMfkEjLL9C97BWMAAAAAQUIPAAAAAAD6tEen2HOpuoYIBL2I7La6; _ga=GA1.2.1051632509.1661303600; _gid=GA1.2.833989407.1661303600; CFID=13513062; CFTOKEN=3d21bc967720d881-3CAD2EA0-D25F-FAEF-ABFEFCFD0EB44BD3; MIGCSHIPPER9384=ANONYMOUS; SHIPPER=ANONYMOUS; incap_ses_969_2551=X/kVFQ60ojaD+RhYDJVyDdDgBmMAAAAA29hUbXlRTl6zqAXwG5/LVQ==',
        'Host': 'www.gasnom.com',
        'Origin': 'http://www.gasnom.com',
        'Referer': 'http://www.gasnom.com/ip/kinetica/transposting.cfm?id=1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36v'
    }

    def __init__(self, job_id):
        PipelineScraper.__init__(self, job_id, web_url=self.api_url, source=self.source)

    def get_payload(self, post_date: date = None):

        payload = {
            'qry': '1',
            'frmEffectiveDt': self.current_post_date,
            'FRMENDDT': self.current_post_date,
            'B1': 'Download'
        }

        return payload

    def start_scraping(self, post_date: date = None):
        """

        :param post_date:
        :return:
        """
        try:
            logger.info('Scraping %s pipeline gas for post date: %s', self.source, self.current_post_date)
            payload = self.get_payload(post_date)
            response = self.session.post(self.post_data_url, data=payload, headers=self.post_page_headers)
            response.raise_for_status()

            html_text = response.text

            csv_data = StringIO(html_text)
            df_result = pandas.read_csv(csv_data, sep='\t')
            self.save_result(df_result, post_date=post_date, local_file=True)       

            logger.info('File saved. End of scraping: %s', self.source)
        except Exception as ex:
            logger.error(ex, exc_info=True)

        return None


def back_fill_pipeline_date():
    scraper = GasNom(job_id=str(uuid.uuid4()))
    for i in range(90, -1, -1):
        post_date = (date.today() - timedelta(days=i))
        print(post_date)
        scraper.start_scraping(post_date)


def main():
    scraper = GasNom(job_id=str(uuid.uuid4()))
    scraper.start_scraping()
    scraper.scraper_info()


if __name__ == '__main__':
    main()
