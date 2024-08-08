import os, string, shutil, time, json, urllib

import datetime as dt 
import pandas   as pd 

from config.DBInterfacePostgres  import DBInterface 
from config.SeleniumSettings     import SeleniumSettings 
from config.EnvironmentVariables import PDF_COLLECTOR_USERNAME, PDF_COLLECTOR_PASSWORD 

class MarketingReportsCollector:
    def __init__(self, selenium_object: SeleniumSettings):
        self.selenium_object = selenium_object 
        self.current_date    = str(dt.datetime.now().date())
        self.selenium_object.driver_settings()

    def market_report_collector_settings_method(self, download_path: str, output_file_name: str, pdf_data_table_name: str, pdf_data_table_column: str, sql_type: str, hostname: str, server_name: str, target_report_types: list[str], incross_attributes: dict, mezzo_attributes: dict, nas_attributes: dict):                                               
        self.download_path         = download_path
        self.output_file_name      = output_file_name.format(self.current_date)
        self.pdf_data_table_name   = pdf_data_table_name
        self.pdf_data_table_column = pdf_data_table_column
        self.target_report_types   = target_report_types
        self.incross_attributes    = incross_attributes 
        self.mezzo_attributes      = mezzo_attributes
        self.nas_attributes        = nas_attributes 
        self.__database_object     = DBInterface()
        self.__database_object.connection_settings(sql_type, os.getenv(PDF_COLLECTOR_USERNAME), os.getenv(PDF_COLLECTOR_PASSWORD), hostname, server_name)

        self.pdf_file_data_dictionary = {
            "article_title"    : [],
            "article_date"     : [],
            "article_link"     : [],
            "article_old_file" : [],
            "article_new_file" : [],
            "article_source"   : [],
            "article_tag"      : []
        }

        for attribute_dictionary in [self.incross_attributes, self.mezzo_attributes, self.nas_attributes]:
            os.makedirs(attribute_dictionary["output_directory"].format(self.current_date), exist_ok = True)

    def __incross_collector(self):
        article_titles = [self.selenium_object.wait_for_element_and_return_element(f"/html/body/div/div[6]/div[2]/a[{article_index}]/div/div[2]/dl/dt/div", "xpath").text for article_index in range(1, 13)]
        article_dates  = [self.selenium_object.wait_for_element_and_return_element(f"/html/body/div/div[6]/div[2]/a[{article_index}]/div/div[2]/p", "xpath").text for article_index in range(1, 13)]
        article_links  = [self.selenium_object.wait_for_element_and_return_element(f"/html/body/div/div[6]/div[2]/a[{article_index}]", "xpath").get_attribute("href") for article_index in range(1, 13)]
        article_tags   = [self.selenium_object.wait_for_element_and_return_element(f"/html/body/div/div[6]/div[2]/a[{article_index}]/div/div[2]/dl/dt/p", "xpath").text for article_index in range(1, 13)]

        return article_titles, article_dates, article_links, article_tags        

    def __mezzo_collector(self):
        article_titles = [article_object.text for article_object in self.selenium_object.wait_for_elements_and_return_elements("ellipsis_title")]
        article_dates  = [tag_object.text[0:10] for tag_object in self.selenium_object.wait_for_elements_and_return_elements("ellipsis_tag")]
        article_links  = [download_button.get_attribute("href") for download_button in self.selenium_object.wait_for_elements_and_return_elements("myDown")]
        article_tags   = ["" for _ in article_titles]

        return article_titles, article_dates, article_links, article_tags     

    def __nas_collector(self):
        article_titles = [article_object.text for article_object in self.selenium_object.search_for_elements("jt_grid_list_title")]
        article_dates  = [date_object.text.replace(".", "-") for date_object in self.selenium_object.search_for_elements("jt_grid_list_date")]
        article_links  = [urllib.parse.unquote(link_object.get_attribute("href")) for link_object in self.selenium_object.search_for_elements("jt_grid_list_item.jt_grid_list_with_thumb")]
        article_tags   = ["" for _ in article_titles]

        return article_titles, article_dates, article_links, article_tags

    def collect_pdf_info(self):
        def check_if_article_exists_in_database(article_source: str, table_name: str, title_column: str, article_titles: list[str], article_dates: list[str], article_links: list[str], article_tags: list[str]):
            exists_yn = False 

            for (title, upload_date, download_url, article_tag) in zip(article_titles, article_dates, article_links, article_tags):
                if (self.__database_object.check_if_data_exists_in_column(table_name, title_column, title)[0][0] == 0):
                    if (article_tag in self.target_report_types + [""]):
                        self.pdf_file_data_dictionary["article_title"].append(title)
                        self.pdf_file_data_dictionary["article_date"].append(upload_date)
                        self.pdf_file_data_dictionary["article_link"].append(download_url)
                        self.pdf_file_data_dictionary["article_source"].append(article_source)
                        self.pdf_file_data_dictionary["article_tag"].append(article_tag)
                else:
                    exists_yn = True 
                    break 
            
            return exists_yn 
        
        def inner_while_loop(page_number: int, attribute_dictionary: dict):            
            while True:
                self.selenium_object.driver.get(attribute_dictionary["website_url"].format(page_number))
                page_number += 1

                match attribute_dictionary["article_source"]:
                    case "incross_media" : article_titles, article_dates, article_links, article_tags = self.__incross_collector()
                    case "mezzo_media"   : article_titles, article_dates, article_links, article_tags = self.__mezzo_collector()
                    case "nas_media"     : article_titles, article_dates, article_links, article_tags = self.__nas_collector()
                    case _               : raise Exception("Unknown source type")
                
                if (check_if_article_exists_in_database(attribute_dictionary["article_source"], self.pdf_data_table_name, self.pdf_data_table_column, article_titles, article_dates, article_links, article_tags)):
                    break

        for attribute_dictionary in [self.incross_attributes, self.mezzo_attributes, self.nas_attributes]:
            inner_while_loop(1, attribute_dictionary)

    def download_pdf_files(self):
        def move_pdf_file_to_output_path(article_title: str, attribute_dictionary: dict):
            time.sleep(20)
            old_file_name    = [file_name for file_name in os.listdir(self.download_path) if (file_name.lower()[-4:] == ".pdf")][0]
            new_file_name    = "".join([character for character in article_title if (character not in string.punctuation)]) + ".pdf"
            old_article_path = os.path.join(self.download_path, old_file_name)
            new_article_path = os.path.join(attribute_dictionary["output_directory"].format(self.current_date), new_file_name)
            shutil.move(old_article_path, new_article_path)

            for (file_name, dictionary_key) in zip([old_file_name, new_file_name], ["article_old_file", "article_new_file"]):
                self.pdf_file_data_dictionary[dictionary_key].append(file_name)

        def return_attribute_dictionary(article_source: str):
            match article_source:
                case "incross_media": 
                    self.selenium_object.driver.get(self.selenium_object.wait_for_element_and_return_element("/html/body/div/div[5]/div[1]/table/tbody/tr/td/div[3]/a", "xpath").get_attribute("href"))
                    attribute_dictionary = self.incross_attributes
                case "nas_media": 
                    self.selenium_object.wait_for_element_and_return_element("jt_btn_icon.jt_icon_download.jt_type_03.jt_btn_spread").click()
                    attribute_dictionary = self.nas_attributes
                case "mezzo_media": 
                    attribute_dictionary = self.mezzo_attributes
                    pass 
                case _:
                    raise Exception("Unknown source type")
            
            return attribute_dictionary

        for (article_source, article_title, article_link) in zip(self.pdf_file_data_dictionary["article_source"], self.pdf_file_data_dictionary["article_title"], self.pdf_file_data_dictionary["article_link"]):
            self.selenium_object.driver.get(article_link)
            move_pdf_file_to_output_path(article_title, return_attribute_dictionary(article_source))
    
    def save_pdf_file_data(self):
        output_dataframe = pd.DataFrame(self.pdf_file_data_dictionary)
        self.__database_object.upload_to_database(self.pdf_data_table_name, output_dataframe)

        match self.output_file_name.split(".")[-1].lower():
            case "xlsx" : output_dataframe.to_excel(self.output_file_name, index = False)
            case "csv"  : output_dataframe.to_csv(self.output_file_name, index = False, encoding = "utf-8")
            case _      : raise Exception("Unrecognized file format")

if (__name__ == "__main__"):
    with open("./config/PDFDistributionConfig.json", "r", encoding = "utf-8") as f:
        config_dict = json.load(f)
    
    marketing_report_collector = MarketingReportsCollector(SeleniumSettings(**config_dict["PDFDistributionPipeline"]["constructor"]))
    marketing_report_collector.market_report_collector_settings_method(**config_dict["PDFDistributionPipeline"]["market_report_collector_settings_method"])
    marketing_report_collector.collect_pdf_info()
    marketing_report_collector.download_pdf_files()
    marketing_report_collector.save_pdf_file_data()
