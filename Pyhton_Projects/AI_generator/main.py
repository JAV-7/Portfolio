import customtkinter as ctk #pip install customtkinter
import tkinter as tk
import urllib.request #With io, is used to set background image
import io
import ai
from PIL import Image #Used for background image

#Generate an instance of Generate class
generate_instance = ai.Generate()

def generate():
    try:
        user_prompt = prompt_entry.get("0.0", tk.END).strip()#.strip() to remove exceeding spaces at start and end
        style = style_dropdown.get()

        #If empty
        if not user_prompt:
            raise ValueError("Prompt cannot be empty. Please enter a valid prompt.")
        

        generate_instance.set_prompt(f"{user_prompt} in style: {style}")
        generate_instance.set_number(number_slider.get())
        canvas_images = generate_instance.get_images()
        
        if canvas_images is None or len(canvas_images) == 0:
            raise ValueError("No images were generated. Please check your prompt and try again.")

        #Update images from canvas
        def update_image(index=0):
            canvas.image = canvas_images[index]
            canvas.create_image(0, 0, anchor = "nw", image = canvas_images[index])
            index = (index + 1) % len(canvas_images) # Move across valid range
            canvas.after(3000, update_image, index) #'Recursive' way of calling the function back
        
        update_image()

    except Exception as e:
        problem_window = ctk.CTk()
        problem_window.title("Problem")
        problem_window.geometry("300x100")

        #Problem label
        problem_label = ctk.CTkLabel(problem_window, text= f"Error: {e}")
        problem_label.pack(pady=20, padx=20)
        problem_window.mainloop()

root = ctk.CTk() #Create app
root.title("AI Image Generator")
#root.geometry("800x600")

ctk.set_appearance_mode("system") #Set the GUI to system preferences

#Background image
url = 'https://th.bing.com/th/id/OIP.C056RBnQ-khsyov9gyleKAHaL2?rs=1&pid=ImgDetMain'
image_data = urllib.request.urlopen(url).read() #Retrieve image data
background_image = Image.open(io.BytesIO(image_data)) #Get image
background_ctk_image = ctk.CTkImage(light_image=background_image, dark_image=background_image, size=(450,720))

#Add background image
background_label = ctk.CTkLabel(root, image=background_ctk_image, text = None)
background_label.place(x = 0, y = 0)

#Create frame
input_frame = ctk.CTkFrame(root) #Create a frame, 'root' is the parent widget of the frame
input_frame.pack(side = "left", expand = True, padx = 20, pady = 20)#'.pack' allows resizing

#Prompt row
prompt_label= ctk.CTkLabel(input_frame, text="Prompt")
prompt_label.grid(row = 0, column = 0, padx = 10, pady = 10)#Add the label to the gui

prompt_entry = ctk.CTkTextbox(input_frame, height = 10)
prompt_entry.grid(row = 0, column = 1, padx = 10, pady = 10)

#Style row
style_label= ctk.CTkLabel(input_frame, text="Style")
style_label.grid(row = 1, column = 0, padx = 10, pady = 10)
#Dropdown to choose the style of the image
style_dropdown =ctk.CTkComboBox(input_frame, values=["Photorealistic", "Cartoon", "Abstract", "Surreal", "Minimalist", "Vintage", "Fantasy", "Watercolor", "Sketch", "3D Render"])
style_dropdown.grid(row = 1, column = 1, padx = 10, pady = 10)

#Number row
number_label = ctk.CTkLabel(input_frame, text="# Images")
number_label.grid(row = 2, column = 0, padx = 10, pady= 10) 

#Number slider
number_slider = ctk.CTkSlider(input_frame, from_ = 1, to = 10, number_of_steps = 9)
number_slider.grid(row = 2, column = 1, padx = 10, pady = 10)

#Note label
italic_font = ctk.CTkFont(family="Helvetica", size=10, slant="italic")
#Create a font for the label 
note_label = ctk.CTkLabel(input_frame, text="Select from 1 to 10 images to generate.", font = italic_font)
note_label.grid(row = 3, column = 0, columnspan = 2, padx = 10, pady = 10)


#Generate button
generate_button = ctk.CTkButton(input_frame, text="Generate", command = generate)
generate_button.grid(row = 4, column = 0, columnspan = 2, sticky = "news", padx = 10, pady = 10)


#Canvas
canvas = tk.Canvas(root, width = 512, height = 512)
canvas.pack(side="left")

root.mainloop()#Keeps running the app