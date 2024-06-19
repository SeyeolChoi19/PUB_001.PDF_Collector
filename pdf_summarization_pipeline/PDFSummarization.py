import os, json

import datetime as dt 

from openai import OpenAI 

from config.EnvironmentVariables import OPENAI_API_KEY
from config.EnvironmentVariables import OPENAI_ORG_KEY

class PDFSummarization:
    def __init__(self):
        self.current_date = "2024-06-17"

    def pdf_summarization_settings_method(self, incross_media_json_path: str, mezzo_media_json_path: str, nas_media_json_path: str, gpt_model_name: str):
        self.incross_media_json_path = incross_media_json_path 
        self.mezzo_media_json_path   = mezzo_media_json_path 
        self.nas_media_json_path     = nas_media_json_path 
        self.gpt_model_name          = gpt_model_name

        self.__api_object = OpenAI(
            organization = os.getenv(OPENAI_ORG_KEY),
            api_key      = os.getenv(OPENAI_API_KEY)
        )

    def __json_file_io(self, file_name: str, operation_flag: str, file_contents_dict: dict = None):
        with open(file_name, operation_flag, encoding = "utf-8") as f:
            if (operation_flag == "r"):
                return json.load(f)
            elif (operation_flag == "w"):
                json.dump(file_contents_dict, indent = 4, ensure_ascii = False)

    def __text_summarization(self, page_content: str, page_file_flag: str = "page"):
        character_limit = 150 if (page_file_flag == "page") else 4000 

        response = self.__api_object.chat.completions.create(
            model    = self.gpt_model_name,
            messages = [{"role" : "user", "content" : f"Summarize this text within {character_limit} characters in Korean. Don't give me anything else besides the summary : {page_content}"}]
        ).choices[0].message.content

        return response 

    def read_and_summarize_file_contents(self):                     
        def file_and_page_wise_summarization(json_files_list: list[str]):
            for file_name in json_files_list:
                file_contents_dict                 = self.__json_file_io(file_name, "r")
                page_wise_text_summaries           = [self.__text_summarization(page_content, "page") for page_content in file_contents_dict["file_contents"].values()]
                file_summarization_string          = self.__text_summarization("\n".join(page_wise_text_summaries), "file")
                file_contents_dict["file_summary"] = file_summarization_string
                self.__json_file_io(file_name, "w", file_contents_dict)

        for file_path in [self.incross_media_json_path, self.mezzo_media_json_path, self.nas_media_json_path]:
            file_input_path = file_path.format(self.current_date)
            json_files_list = [os.path.join(file_input_path, file_name) for file_name in os.listdir(file_input_path)]           
            file_and_page_wise_summarization(json_files_list)

if (__name__ == "__main__"):
    with open("./config/PDFDistributionConfig.json", "r", encoding = "utf-8") as f:
        config_dict = json.load(f)

    pdf_summarization = PDFSummarization()
    pdf_summarization.pdf_summarization_settings_method(**config_dict["PDFDistributionPipeline"]["pdf_summarization_settings_method"])
    pdf_summarization.read_and_summarize_file_contents()
