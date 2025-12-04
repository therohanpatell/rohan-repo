import csv
import json
import random
from collections import OrderedDict

def convert_csv_to_json(csv_path, output_path):
    # Read the CSV file
    with open(csv_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        questions = []
        
        for row in reader:
            # Create options list with empty string as first element
            options = [
                "",
                row['Option_A'],
                row['Option_B'],
                row['Option_C'],
                row['Option_D']
            ]
            
            # Get the correct answer letter and convert to index (A=1, B=2, C=3, D=4)
            correct_letter = row['Correct_Answer']
            correct_index = ord(correct_letter) - ord('A') + 1
            
            # Extract the actual correct answer text
            correct_text = options[correct_index]
            
            # Create a list of the actual options (without the empty string)
            actual_options = options[1:]
            
            # Shuffle the options but keep track of where the correct answer goes
            shuffled_options = actual_options.copy()
            random.shuffle(shuffled_options)
            
            # Find the new position of the correct answer
            new_correct_index = shuffled_options.index(correct_text) + 1  # +1 because of the empty string
            
            # Create the new options list with empty string at the beginning
            new_options = [""] + shuffled_options
            
            # Convert index back to letter (1=A, 2=B, 3=C, 4=D)
            new_correct_letter = chr(new_correct_index + ord('A') - 1)
            
            # Create the question dictionary with proper ordering
            question = OrderedDict([
                ("question", row['Question']),
                ("correctAnswer", new_correct_letter),
                ("difficulty", row['Difficulty']),
                ("questionNumber", int(row['Question_Number'])),
                ("options", new_options),
                ("world", row['World'])
            ])
            
            questions.append(question)
    
    # Write to JSON file with proper encoding and formatting
    with open(output_path, 'w', encoding='utf-8') as jsonfile:
        json.dump(questions, jsonfile, ensure_ascii=False, indent=4)
    
    print(f"Successfully converted {len(questions)} questions to JSON format.")
    print(f"Output saved to: {output_path}")

# Example usage:
if __name__ == "__main__":
    convert_csv_to_json('ALL_QUESTIONS_MASTER.csv', 'questions.json')
