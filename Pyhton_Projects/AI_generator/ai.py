#This file is in charge of generating the AI images
import customtkinter as ctk
import openai #pip install openai
import os #retrieve API from environment
import requests,io
from PIL import Image, ImageTk

openai.api_key = os.getenv('OPENAI_API_KEY')
#Retrieve the api key from the environment variable

"""
The api key is stored as an environment variable due to security reasons.
You can get your api key from the OpenAI dashboard following in your browser:
platform.openai.com->"sign in"->platform.openai.com/api-keys->"create new secret key"

Following that, you can store your api key in your system environment by selecting advanced->create a
new environment variable
"""
class Generate:

    def __init__(self):
        self.user_prompt = ""
        self.number = 0

    def set_prompt(self, string):
        self.user_prompt = string #What is the generated image

    def set_number(self, n):
        self.number = n
    
    def get_images(self):
        try:
            response = openai.Image.create(
                prompt= self.user_prompt,
                n= int(self.number), #Number of images to generate
                size="512x512" #Size of the generated image
            )

            image_urls = []
            for i in range(len(response['data'])):
                image_urls.append(response['data'][i]['url']) 

            images = []
            for url in image_urls:
                response = requests.get(url)
                image = Image.open(io.BytesIO(response.content))
                photo_image = ImageTk.PhotoImage(image)
                images.append(photo_image)

            return images
        
            """Get the data from the response, sub num to refer to the image
            that is generated, and url is the link of the image returned by the api.
            As we are managing multiple images, the for loop helps to retrieve
            the selcted 'n' images"""

        #This secton is exception handling, creating error windows if they happen by calling a function
        except openai.error.InvalidRequestError as e:
            self.show_error(f"Error: {e}")

        except requests.exceptions.RequestException as e:
            self.show_error(f"Request error: {e}")

        except Exception as e:
                self.show_error(f"An unexpected error occurred: {e}")
                    

    def show_error(self, message):
        #Error window
        error_window = ctk.CTk()
        error_window.title("Error")
        error_window.geometry("300x100")

        #Error label
        error_label = ctk.CTkLabel(error_window, text = message)
        error_label.pack(pady=20, padx=20)
        error_window.mainloop()
         



            
