import os

result = ""

for file in os.listdir('logs'):
    with open(f'logs/{file}', 'r') as f:
        result += f.read()

with open('logs/result.log', 'w') as f:
    f.write(result)
