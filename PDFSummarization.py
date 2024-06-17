import os

from openai import OpenAI 

api_object = OpenAI(
    organization = "organization id",
    api_key      = os.getenv("OPENAI_API_KEY")
)

response = api_object.chat.completions.create(
    model    = "gpt-4",
    messages = [{"role" : "user", "content" : "Say this is a test" }]
)
