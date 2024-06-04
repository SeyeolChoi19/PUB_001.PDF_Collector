import os, json, string, shutil, time

import datetime as dt 
import pandas   as pd

from config.DBInterfacePostgres  import DBInterface
from config.SeleniumSettings     import SeleniumSettings 
from config.EnvironmentVariables import PDF_COLLECTOR_USERNAME, PDF_COLLECTOR_PASSWORD

class MezzoMediaCollector:
    def __init__(self, selenium_object: SeleniumSettings):
        self.selenium_object = selenium_object 
        self.selenium_object.driver_settings()
    
    def mezzo_media_settings_method(self, mezzo_media_url: str, output_file_name: str, output_directory: str, download_path: str, mezzo_media_table: str, article_title_column: str, sql_type: str, hostname: str, server_name: str):
        self.mezzo_media_url      = mezzo_media_url
        self.output_file_name     = output_file_name.format(str(dt.datetime.now().date()))
        self.output_directory     = os.path.join(output_directory, str(dt.datetime.now().date()))
        self.download_path        = download_path
        self.server_name          = server_name 
        self.mezzo_media_table    = mezzo_media_table
        self.article_title_column = article_title_column
        self.__database_object    = DBInterface()
        self.__database_object.connection_settings(sql_type, os.getenv(PDF_COLLECTOR_USERNAME), os.getenv(PDF_COLLECTOR_PASSWORD), hostname, server_name)
        
        self.mezzo_media_data_dictionary = {
            "mezzo_media_article_title"    : [],
            "mezzo_media_article_date"     : [],
            "mezzo_media_article_link"     : [],
            "mezzo_media_article_old_file" : [],
            "mezzo_media_article_new_file" : []
        }

        os.makedirs(self.output_directory, exist_ok = True)

    def __get_reference_data(self):
        self.__reference_data = self.__database_object.get_from_database(self.mezzo_media_table, [self.article_title_column])
        self.__reference_data = [row[0] for row in self.__reference_data]

    def collect_mezzo_media_pdf_info(self):
        def check_if_article_exists_in_database(article_titles: list[str], article_dates: list[str], article_links: list[str]):
            exists_yn = False

            for (title, upload_date, download_url) in zip(article_titles, article_dates, article_links):
                if (title not in self.__reference_data):
                    self.mezzo_media_data_dictionary["mezzo_media_article_title"].append(title)
                    self.mezzo_media_data_dictionary["mezzo_media_article_date"].append(upload_date)
                    self.mezzo_media_data_dictionary["mezzo_media_article_link"].append(download_url)
                else:
                    exists_yn = True
                    break
                
            return exists_yn          

        page_number = 1
        self.__get_reference_data()

        while True:
            self.selenium_object.driver.get(self.mezzo_media_url.format(page_number))
            article_titles = [article_object.text for article_object in mezzo_media_collector.selenium_object.search_for_elements("ellipsis_title")]
            article_dates  = [tag_object.text[0:10] for tag_object in mezzo_media_collector.selenium_object.search_for_elements("ellipsis_tag")]
            article_links  = [download_button.get_attribute("href") for download_button in mezzo_media_collector.selenium_object.search_for_elements("myDown")]
            exists_yn      = check_if_article_exists_in_database(article_titles, article_dates, article_links)
            page_number   += 1

            if (exists_yn == True):
                break

    def download_mezzo_media_pdf_files(self):
        def move_pdf_file_to_output_path(article_title: str):
            time.sleep(6)
            old_article_path = os.path.join(self.download_path, [file_name for file_name in os.listdir(self.download_path) if (file_name.lower()[-4:] == ".pdf")][0])
            new_article_path = os.path.join(self.output_directory, "".join([character for character in article_title if (character not in string.punctuation)])) + ".pdf"
            shutil.move(old_article_path, new_article_path)

            for (file_name, dictionary_key) in zip([old_article_path, new_article_path], ["mezzo_media_article_old_file", "mezzo_media_article_new_file"]):
                self.mezzo_media_data_dictionary[dictionary_key].append(file_name)
            
        for (article_title, article_link) in zip(self.mezzo_media_data_dictionary["mezzo_media_article_title"], self.mezzo_media_data_dictionary["mezzo_media_article_link"]):
            self.selenium_object.driver.get(article_link)
            move_pdf_file_to_output_path(article_title)

if (__name__ == "__main__"):
    with open("./config/PDFDistributionConfig.json", "r", encoding = "utf-8") as f:
        config_dict = json.load(f)

    mezzo_media_collector = MezzoMediaCollector(SeleniumSettings(**config_dict["PDFDistributionPipeline"]["MezzoMediaCollector"]["constructor"]))
    mezzo_media_collector.mezzo_media_settings_method(**config_dict["PDFDistributionPipeline"]["MezzoMediaCollector"]["mezzo_media_settings_method"])
    mezzo_media_collector.collect_mezzo_media_pdf_info()
    mezzo_media_collector.download_mezzo_media_pdf_files()
