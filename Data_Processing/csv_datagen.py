import csv
from faker import Faker

# generate_csv function : Generates a CSV file with fake data - realistic data.
def generate_csv(file_name, num_rows):
    fake = Faker()

    # opens the file in write mode to write the data
    with open(file_name, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["First_name", "Last_name", "Address", "Date_of_birth"])
        for i in range(num_rows):
            writer.writerow([  # writes the data(fake date getting generated by faker() to each row)
                fake.first_name(),
                fake.last_name(),
                fake.address().replace('\n', ', '), # address includes street city state and zipcode separated by ','
                fake.date_of_birth(minimum_age=10, maximum_age=100).strftime("%m/%d/%Y") # date of birth is formatted as mm/dd/yyyy with age range of 10 to 100 years
            ])

if __name__ == "__main__":

    # this variable is assigned the path in the local computer where the data is to be saved for further processing.
    output_path = "C:\\*\\sample_data.csv"
    generate_csv(output_path, 10**7) # ~2GB data
