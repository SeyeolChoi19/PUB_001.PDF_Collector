import os, json

import datetime as dt 

from openai import OpenAI 

from config.EnvironmentVariables import OPENAI_API_KEY
from config.EnvironmentVariables import OPENAI_ORG_KEY

class PDFSummarization:
    def __init__(self):
        self.current_date = str(dt.datetime.now().date())

    def pdf_summarization_settings_method(self, incross_media_json_path: str, mezzo_media_json_path: str, nas_media_json_path: str, gpt_model_name: str):
        self.incross_media_json_path = incross_media_json_path 
        self.mezzo_media_json_path   = mezzo_media_json_path 
        self.nas_media_json_path     = nas_media_json_path 
        self.gpt_model_name          = gpt_model_name

        self.__api_object = OpenAI(
            organization = os.getenv(OPENAI_ORG_KEY),
            api_key      = os.getenv(OPENAI_API_KEY)
        )

    def __summarization(self, contents_list: list[str]):
        while (len(contents_list) > 1): 
            chunked_objects = self.__chunk_file_text(contents_list)
            contents_list   = chunked_objects[1]
            summarized_text = self.__api_object.chat.completion.create(
                model    = self.gpt_model_name, 
                messages = [{"role" : "user", "content" : f"Summarize this text within 1000 characters in Korean : {chunked_objects[0]}"}]
            ).choices[0].message.content

            if (len(contents_list) > 1):
                contents_list.insert(0, summarized_text)
    
        return chunked_objects[0]
    
    def __chunk_file_text(self, contents_list: list[str]):
        file_string = ""

        for file_text in contents_list:
            if (len(file_string) < 5000):
                file_string += file_text 
                contents_list.remove(file_text)
        
        return file_string, contents_list     

    def read_and_summarize_file_contents(self):
        def get_file_contents(json_files_list: list[str]):
            for file_name in json_files_list:
                with open(file_name, "r", encoding = "utf-8") as f:
                    file_contents = json.load(f)

                file_contents["file_summary"] = self.__summarization(list(file_contents["file_contents"].values()))

                with open(file_name, "w", encoding = "utf-8") as f:
                    json.dump(file_contents, f, indent = 4, ensure_ascii = False)
                         
        for file_path in [self.incross_media_json_path, self.mezzo_media_json_path, self.nas_media_json_path]:
            file_input_path = file_path.format(self.current_date)
            json_files_list = [os.path.join(file_input_path, file_name) for file_name in os.listdir(file_input_path)]
            get_file_contents(json_files_list)

