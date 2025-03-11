#Please insert EVERYTHING using GUI
#pip install tabulate
#Please use parentheses, ⨉ p∨q∨r ✓ (p∨q)∨r
#Even for negatives: ⨉ ¬q⋀¬p ✓ (¬q)⋀(¬p)
# ⨉ ¬(p⋀¬q⋀s) ✓ ¬(p⋀((¬q)⋀s)) or ¬((p⋀(¬q))⋀s)
from tkinter import *;
from tkinter import scrolledtext;
from itertools import product
import Portfolio.Pyhton_Projects.Boolean_Algebra.boolean_algebra as boolean_algebra


i = 0
#Calculate process via relational_algebra.py
def run_calculation():
    expression = initial_text.get()
    result = boolean_algebra.calculate(expression)  # Call calculate()
    table.delete("1.0", END)
    table.insert("end", result)

#Color function
def change_to_white(button):
    button.bind("<Enter>", func=lambda e: button.config(
        background='#F9813A', fg = '#1A1C20'))
 
    button.bind("<Leave>", func=lambda e: button.config(
        background='#F9813A', fg = '#FCF1F1'))
    
#Buttons functions
def click_add(boton):
    global i
    initial_text.insert(i,boton)
    i+=1
    return i

def click_delete():
    global i
    if i>0:
        i-=1
        initial_text.delete(i)
        return i
    else:
        initial_text.delete(0)

def click_truncate():
    global i
    initial_text.delete(0,END)
    i = 0
    return i

#Create window

window = Tk()
window.title("TRUTH TABLE GEENRATOR.")
window.config(bg='white')
window.geometry("1197x560")

table = scrolledtext.ScrolledText(window, width=100, height=5)
table.grid(row=2, column=0, columnspan=13, padx=10, pady=10)

initial_text = Entry(window, font=("Helvetica 11 bold"), bg='white', fg= '#F9813A', borderwidth= 0, highlightbackground= '#1A1C20', justify='center', width= 100)
initial_text.grid(row = 1, column= 0, columnspan= 13, padx = 25, pady= 10)



#Primitive buttons
button_a = Button(window, text = "a", width = 5, height= 2, font=("Helvetica 11 bold"), activebackground='#DBDBDB', borderwidth=0, command= lambda: click_add("a"))
button_b = Button(window, text = "b", width = 5, height= 2, font=("Helvetica 11 bold"), activebackground='#DBDBDB', borderwidth=0, command= lambda: click_add("b"))
button_c = Button(window, text = "c", width = 5, height= 2, font=("Helvetica 11 bold"), activebackground='#DBDBDB', borderwidth=0, command= lambda: click_add("c"))
button_d = Button(window, text = "d", width = 5, height= 2, font=("Helvetica 11 bold"), activebackground='#DBDBDB', borderwidth=0, command= lambda: click_add("d"))

button_e = Button(window, text = "e", width = 5, height= 2, font=("Helvetica 11 bold"), activebackground='#DBDBDB', borderwidth=0, command= lambda: click_add("e"))
button_f = Button(window, text = "f", width = 5, height= 2, font=("Helvetica 11 bold"), activebackground='#DBDBDB', borderwidth=0, command= lambda: click_add("f"))
button_g = Button(window, text = "g", width = 5, height= 2, font=("Helvetica 11 bold"), activebackground='#DBDBDB', borderwidth=0, command= lambda: click_add("g"))
button_h = Button(window, text = "h", width = 5, height= 2, font=("Helvetica 11 bold"), activebackground='#DBDBDB', borderwidth=0, command= lambda: click_add("h"))

button_i = Button(window, text = "i", width = 5, height= 2, font=("Helvetica 11 bold"), activebackground='#DBDBDB', borderwidth=0, command= lambda: click_add("i"))
button_j = Button(window, text = "j", width = 5, height= 2, font=("Helvetica 11 bold"), activebackground='#DBDBDB', borderwidth=0, command= lambda: click_add("j"))
button_k = Button(window, text = "k", width = 5, height= 2, font=("Helvetica 11 bold"), activebackground='#DBDBDB', borderwidth=0, command= lambda: click_add("k"))
button_l = Button(window, text = "l", width = 5, height= 2, font=("Helvetica 11 bold"), activebackground='#DBDBDB', borderwidth=0, command= lambda: click_add("l"))

button_m = Button(window, text = "m", width = 5, height= 2, font=("Helvetica 11 bold"), activebackground='#DBDBDB', borderwidth=0, command= lambda: click_add("m"))
button_n = Button(window, text = "n", width = 5, height= 2, font=("Helvetica 11 bold"), activebackground='#DBDBDB', borderwidth=0, command= lambda: click_add("n"))
button_o = Button(window, text = "o", width = 5, height= 2, font=("Helvetica 11 bold"), activebackground='#DBDBDB', borderwidth=0, command= lambda: click_add("o"))

button_p = Button(window, text = "p", width = 5, height= 2, font=("Helvetica 11 bold"), activebackground='#DBDBDB', borderwidth=0, command= lambda: click_add("p"))
button_q = Button(window, text = "q", width = 5, height= 2, font=("Helvetica 11 bold"), activebackground='#DBDBDB', borderwidth=0, command= lambda: click_add("q"))
button_r = Button(window, text = "r", width = 5, height= 2, font=("Helvetica 11 bold"), activebackground='#DBDBDB', borderwidth=0, command= lambda: click_add("r"))
button_s = Button(window, text = "s", width = 5, height= 2, font=("Helvetica 11 bold"), activebackground='#DBDBDB', borderwidth=0, command= lambda: click_add("s"))

button_t = Button(window, text = "t", width = 5, height= 2, font=("Helvetica 11 bold"), activebackground='#DBDBDB', borderwidth=0, command= lambda: click_add("t"))
button_u = Button(window, text = "u", width = 5, height= 2, font=("Helvetica 11 bold"), activebackground='#DBDBDB', borderwidth=0, command= lambda: click_add("u"))
button_v = Button(window, text = "v", width = 5, height= 2, font=("Helvetica 11 bold"), activebackground='#DBDBDB', borderwidth=0, command= lambda: click_add("v"))
button_w = Button(window, text = "w", width = 5, height= 2, font=("Helvetica 11 bold"), activebackground='#DBDBDB', borderwidth=0, command= lambda: click_add("w"))

button_x = Button(window, text = "x", width = 5, height= 2, font=("Helvetica 11 bold"), activebackground='#DBDBDB', borderwidth=0, command= lambda: click_add("x"))
button_y = Button(window, text = "y", width = 5, height= 2, font=("Helvetica 11 bold"), activebackground='#DBDBDB', borderwidth=0, command= lambda: click_add("y"))
button_z = Button(window, text = "z", width = 5, height= 2, font=("Helvetica 11 bold"), activebackground='#DBDBDB', borderwidth=0, command= lambda: click_add("z"))



#Operators
button_not = Button(window, text = "¬", width = 5, height= 2, font=("Helvetica 11 bold"), bg='white', fg='#F9813A', activebackground='#FFD5CD', borderwidth=0, command= lambda: click_add("¬"))
button_and = Button(window, text = "∧", width = 5, height= 2, font=("Helvetica 11 bold"), bg='white', fg='#F9813A', activebackground='#FFD5CD', borderwidth=0, command= lambda: click_add("∧"))
button_or = Button(window, text = "∨", width = 5, height= 2, font=("Helvetica 11 bold"), bg='white', fg='#F9813A', activebackground='#FFD5CD', borderwidth=0, command= lambda: click_add("∨"))
button_xor = Button(window, text = "⨁", width = 5, height= 2, font=("Helvetica 11 bold"), bg='white', fg='#F9813A', activebackground='#FFD5CD', borderwidth=0, command= lambda: click_add("⨁"))
button_if_then = Button(window, text = "→", width = 5, height= 2, font=("Helvetica 11 bold"), bg='white', fg='#F9813A', activebackground='#FFD5CD', borderwidth=0, command= lambda: click_add("→"))
button_if_and_only_if = Button(window, text = "↔", width = 5, height= 2, font=("Helvetica 11 bold"), bg='white', fg='#F9813A', activebackground='#FFD5CD', borderwidth=0, command= lambda: click_add("↔"))

#Others
button_ac = Button(window, text = "AC", width = 28, height= 2, font=("Helvetica 11 bold"), bg='#F9813A', fg='#FCF1F1', activebackground='#B85F2B',  borderwidth=0, command= lambda: click_truncate())
button_del = Button(window, text = "DEL", width = 20, height= 2, font=("Helvetica 11 bold"), bg='#F9813A', fg='#FCF1F1', activebackground='#B85F2B', borderwidth=0,command= lambda: click_delete())
button_open_parentheses = Button(window, text = "(", width = 5, height= 2, font=("Helvetica 11 bold"), bg='white', fg='#F9813A', activebackground='#FFD5CD', borderwidth=0, command= lambda: click_add("("))
button_close_parentheses = Button(window, text = ")", width = 5, height= 2, font=("Helvetica 11 bold"), bg='white', fg='#F9813A', activebackground='#FFD5CD', borderwidth=0, command= lambda: click_add(")"))
button_equals = Button(window, text = "≡", width = 5, height= 2, font=("Helvetica 11 bold"), bg='#F9813A', fg='#FCF1F1', activebackground='#B85F2B', borderwidth=0, command= lambda: click_add("≡"))
button_send = Button(window, text = "CALCULATE TABLE.", width = 50, height= 2, font=("Helvetica 11 bold"), bg='#F9813A', activebackground='#B85F2B', fg='#FCF1F1',borderwidth=0, command=run_calculation)
space_bar = Label(window, width=5, height=10, bg='white')
instructions_title = Label(window, text = "USAGE INSTRUCTIONS.", width = 25, height= 2, font=("Helvetica 11 bold"), bg='white', fg='#F9813A', borderwidth=0)
instructions_text1 = Label(window, text = "Please use parentheses to connect operations: ⨉ p∨q∨r ✓ (p∨q)∨r", width = 100, height= 2, font=("Helvetica 11 bold"), bg='white', fg='#F9813A', borderwidth=0)
instructions_text2 = Label(window, text = "Please use parentheses when applying negation, unless negating an entire expression: ⨉ ¬q⋀¬p ✓ (¬q)⋀(¬p)", width = 120, height= 2, font=("Helvetica 11 bold"), bg='white', fg='#F9813A', borderwidth=0)
example1 = Label(window, text = " ⨉ ¬(p⋀¬q⋀s) ✓ ¬(p⋀((¬q)⋀s)) o ¬((p⋀(¬q))⋀s)", width = 100, height= 2, font=("Helvetica 11 bold"), bg='white', fg='#F9813A', borderwidth=0, justify='left')
bottom = Label(window, width=170, height=1, bg='#F9813A', borderwidth=0)

#Add buttons to window

#Bottom
bottom.grid(row=12,column=0,columnspan=16, padx=2, pady=2)




#Row 2

button_q.grid(row= 3, column= 0, padx= 2, pady= 5)
button_w.grid(row= 3, column= 1, padx= 2, pady= 5)
button_e.grid(row= 3, column= 2, padx= 2, pady= 5)
button_r.grid(row= 3, column= 3, padx= 2, pady= 5)
button_t.grid(row= 3, column= 4, padx= 2, pady= 5)
button_y.grid(row= 3, column= 5, padx= 2, pady= 5)
button_u.grid(row= 3, column= 6, padx= 2, pady= 5)
button_i.grid(row= 3, column= 7, padx= 2, pady= 5)
button_o.grid(row= 3, column= 8, padx= 2, pady= 5)
button_p.grid(row= 3, column= 9, padx= 2, pady= 5)
button_open_parentheses.grid(row= 3, column= 10, padx= 2, pady= 5)
button_close_parentheses.grid(row= 3, column= 11, padx= 2, pady= 5)

space_bar.grid(row=3, column=12, columnspan= 2, rowspan=4, padx= 2, pady= 5)
button_not.grid(row = 3, column = 14, padx= 2, pady= 5)
button_xor.grid(row = 3, column = 15, padx= 2, pady= 5)

#Row 3
button_a.grid(row= 4, column= 0, padx= 1, pady= 5)
button_s.grid(row= 4, column= 1, padx= 1, pady= 5)
button_d.grid(row= 4, column= 2, padx= 1, pady= 5)
button_f.grid(row= 4, column= 3, padx= 1, pady= 5)
button_g.grid(row= 4, column= 4, padx= 1, pady= 5)
button_h.grid(row= 4, column= 5, padx= 1, pady= 5)
button_j.grid(row= 4, column= 6, padx= 1, pady= 5)
button_k.grid(row= 4, column= 7, padx= 1, pady= 5)
button_l.grid(row= 4, column= 8, padx= 1, pady= 5)
button_del.grid(row= 4, column= 9, columnspan= 3, padx= 5, pady= 2)

button_and.grid(row = 4, column = 14, padx= 2, pady= 5)
button_or.grid(row = 4, column = 15, padx= 2, pady= 5)

#Row 4
button_equals.grid(row= 5, column= 0, padx= 1, pady= 5)
button_z.grid(row= 5, column= 1, padx= 1, pady= 5)
button_x.grid(row= 5, column= 2, padx= 1, pady= 5)
button_c.grid(row= 5, column= 3, padx= 1, pady= 5)
button_v.grid(row= 5, column= 4, padx= 1, pady= 5)
button_b.grid(row= 5, column= 5, padx= 1, pady= 5)
button_n.grid(row= 5, column= 6, padx= 1, pady= 5)
button_m.grid(row= 5, column= 7, padx= 1, pady= 5)
button_ac.grid(row= 5, column= 8, columnspan= 4, padx= 5, pady= 2)

button_if_then.grid(row = 5, column = 14, padx= 2, pady= 5)
button_if_and_only_if.grid(row = 5, column = 15, padx= 2, pady= 5)

#Row 5
button_send.grid(row= 6, column= 1, columnspan= 8, padx= 2, pady= 5)

#Row 8
instructions_title.grid(row = 8, column=3, columnspan= 8, padx = 2, pady= 5)
#Row 9
instructions_text1.grid(row = 9, column=0, columnspan= 10, padx = 2, pady= 2)
#Row 10
instructions_text2.grid(row = 10, column=0, columnspan= 16, padx = 2, pady= 2)
#Row 11
example1.grid(row = 11, column=0, columnspan= 15, padx = 2, pady= 2)


#Functions change color when cursor is on top


change_to_white(button_ac)
change_to_white(button_del)
change_to_white(button_equals)
change_to_white(button_send)





window.mainloop()
