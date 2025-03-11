from itertools import product
from tabulate import tabulate


i = 0
def calculate(expression):
    """
    Takes a logical expression as input and returns the formatted truth table.
    """
    # Validate expression
    if expression.count("(") != expression.count(")") or expression.count("≡") > 1:
        return "Invalid expression. Please check parentheses and operators."

    # Split by equivalence (≡)
    division = expression.split("≡")

    comparative = []
    check_variables = []

    def divide_expressions(string):
        stack = []
        subexpressions = {}
        results = []

        for char in string:
            if char == "(":
                stack.append(len(stack) + 1)
                subexpressions[stack[-1]] = "("
            elif char == ")":
                level = stack.pop()
                subexpressions[level] += ")"
            else:
                if stack:
                    subexpressions[stack[-1]] += char

        for level in sorted(subexpressions.keys(), reverse=True):
            expr = subexpressions[level]
            if expr not in results:
                results.append(expr)

        results.sort(key=lambda x: "not" in x)

        if string not in results:
            results.append(string)

        return results

    def eval_operations(final, variables):
        data = [[] for _ in range(len(final))]
        states = product([True, False], repeat=len(variables))

        information = []
        for state in states:
            instance = dict(zip(variables, state))
            information.append([instance[i] for i in variables])
            for j, exp in enumerate(final):
                valor = eval(exp, instance)
                data[j].append(valor)

        for i in range(len(information)):
            for j in range(len(data)):
                information[i].append(data[j][i])

        return information

    def change_tf(information):
        return [['T' if val else 'F' for val in row] for row in information]

    operators = {"∧": " & ", "∨": " or ", "¬": " not ", "⨁": " ^ ", "→": " <= ", "↔": " == "}

    results = []
    for instance in division:
        format = instance
        variables = sorted(set(filter(str.isalpha, format)))
        check_variables.append(variables)

        for key, value in operators.items():
            format = format.replace(key, value)

        final = divide_expressions(format)
        information = eval_operations(final, variables)
        information_in_tf = change_tf(information)

        headers = ["N°"] + variables + final
        table = tabulate(information_in_tf, headers=headers, showindex="always", numalign="right", stralign="center", tablefmt="tsv")

        results.append(table)

    return "\n\n".join(results)
