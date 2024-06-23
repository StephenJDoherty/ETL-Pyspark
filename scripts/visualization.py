import pandas as pd
import matplotlib.pyplot as plt

def visualize_data(csv_file):
    """
    Visualizes data from a CSV file using pandas and matplotlib.

    Args:
        csv_file (str): Path to the CSV file.

    Returns:
        None
    """

    df = pd.read_csv(csv_file)

    # 1. Gender vs. Disease
    plt.figure(figsize=(10, 6))
    plt.title("Disease Distribution by Gender")
    pd.crosstab(df['Gender'], df['Disease']).plot(kind='bar', stacked=True)
    plt.xlabel("Gender")
    plt.ylabel("Count")
    plt.xticks(rotation=0)
    plt.legend(title="Disease")
    plt.tight_layout()
    plt.show()

    # 2. Age Distribution
    plt.figure(figsize=(8, 5))
    plt.title("Age Distribution")
    plt.hist(df['Age'], bins=10, edgecolor='black')
    plt.xlabel("Age")
    plt.ylabel("Frequency")
    plt.tight_layout()
    plt.show()

    # 3. Treatment Cost vs. Stay Days
    plt.figure(figsize=(8, 5))
    plt.title("Treatment Cost vs. Stay Days")
    plt.scatter(df['StayDays'], df['TreatmentCost'])
    plt.xlabel("Stay Days")
    plt.ylabel("Treatment Cost")
    plt.tight_layout()
    plt.show()

    # 4. City vs. Disease (Bar Chart)
    plt.figure(figsize=(12, 6))
    plt.title("Disease Distribution by City")
    pd.crosstab(df['City'], df['Disease']).plot(kind='bar', stacked=True)
    plt.xlabel("City")
    plt.ylabel("Count")
    plt.xticks(rotation=45)
    plt.legend(title="Disease")
    plt.tight_layout()
    plt.show()

    # 5. Admission Date vs. Discharge Date (Line Plot)
    plt.figure(figsize=(10, 6))
    plt.title("Admission and Discharge Dates")
    plt.plot(df['AdmissionDate'], label='Admission')
    plt.plot(df['DischargeDate'], label='Discharge')
    plt.xlabel("Patient ID")
    plt.ylabel("Date")
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    csv_file = "data/medical_records.csv"  # Replace with your actual CSV file path
    visualize_data(csv_file)
