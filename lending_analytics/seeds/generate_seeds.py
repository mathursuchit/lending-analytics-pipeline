"""
Generate realistic synthetic loan data for the lending analytics dbt project.
Run once: python seeds/generate_seeds.py
"""
import csv
import random
import datetime

random.seed(42)

LOAN_GRADES = ['A', 'B', 'C', 'D', 'E', 'F']
GRADE_WEIGHTS = [0.20, 0.25, 0.22, 0.18, 0.10, 0.05]
PURPOSES = ['debt_consolidation', 'home_improvement', 'small_business',
            'auto', 'medical', 'credit_card', 'education', 'moving', 'vacation']
STATES = ['CA', 'TX', 'NY', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI',
          'NJ', 'VA', 'WA', 'AZ', 'MA', 'TN', 'IN', 'MO', 'MD', 'WI']
# Default rates scale with grade — Grade A is low-risk, Grade F is high-risk
GRADE_STATUS_WEIGHTS = {
    'A': [0.68, 0.04, 0.26, 0.01, 0.01],  # ~5% default
    'B': [0.60, 0.10, 0.27, 0.02, 0.01],  # ~11% default
    'C': [0.52, 0.17, 0.26, 0.03, 0.02],  # ~19% default
    'D': [0.44, 0.24, 0.25, 0.04, 0.03],  # ~27% default
    'E': [0.36, 0.31, 0.24, 0.05, 0.04],  # ~35% default
    'F': [0.28, 0.38, 0.22, 0.06, 0.06],  # ~44% default
}
STATUSES = ['Fully Paid', 'Charged Off', 'Current', 'Late (31-120 days)', 'Default']
TERMS = ['36 months', '60 months']
HOME_OWNERSHIP = ['RENT', 'MORTGAGE', 'OWN', 'OTHER']
HOME_WEIGHTS = [0.40, 0.42, 0.15, 0.03]

GRADE_INT_RATE = {'A': (5.0, 8.5), 'B': (8.5, 12.0), 'C': (12.0, 16.0),
                  'D': (16.0, 20.0), 'E': (20.0, 24.0), 'F': (24.0, 30.0)}

def random_date(start_year=2018, end_year=2024):
    start = datetime.date(start_year, 1, 1)
    end = datetime.date(end_year, 12, 31)
    return start + datetime.timedelta(days=random.randint(0, (end - start).days))

def generate_loans(n=5000):
    rows = []
    for i in range(1, n + 1):
        grade = random.choices(LOAN_GRADES, weights=GRADE_WEIGHTS)[0]
        sub_grade = grade + str(random.randint(1, 5))
        annual_inc = round(random.lognormvariate(11.0, 0.5), 2)
        loan_amnt = round(random.choice([5000, 10000, 15000, 20000, 25000, 30000, 35000, 40000]), 2)
        int_lo, int_hi = GRADE_INT_RATE[grade]
        int_rate = round(random.uniform(int_lo, int_hi), 2)
        term = random.choices(TERMS, weights=[0.65, 0.35])[0]
        months = int(term.split()[0])
        installment = round(loan_amnt * (int_rate/100/12) / (1 - (1 + int_rate/100/12)**(-months)), 2)
        dti = round(random.uniform(5, 40), 2)
        fico = random.randint(580, 850)
        open_acc = random.randint(2, 25)
        delinq_2yrs = random.choices([0, 1, 2, 3], weights=[0.75, 0.15, 0.07, 0.03])[0]
        pub_rec = random.choices([0, 1, 2], weights=[0.90, 0.08, 0.02])[0]
        revol_util = round(random.uniform(0, 99.9), 1)
        total_acc = open_acc + random.randint(0, 15)
        issue_date = random_date()
        status = random.choices(STATUSES, weights=GRADE_STATUS_WEIGHTS[grade])[0]
        purpose = random.choice(PURPOSES)
        addr_state = random.choice(STATES)
        home_ownership = random.choices(HOME_OWNERSHIP, weights=HOME_WEIGHTS)[0]
        emp_length = random.choice(['< 1 year', '1 year', '2 years', '3 years', '4 years',
                                    '5 years', '6 years', '7 years', '8 years', '9 years', '10+ years'])
        funded_amnt = loan_amnt if status != 'Current' else round(loan_amnt * random.uniform(0.5, 1.0), 2)
        total_pymnt = round(funded_amnt * random.uniform(0.1, 1.3), 2) if status == 'Fully Paid' \
                      else round(funded_amnt * random.uniform(0.0, 0.6), 2)

        rows.append({
            'loan_id': f'LC{i:06d}',
            'loan_amnt': loan_amnt,
            'funded_amnt': funded_amnt,
            'term': term,
            'int_rate': int_rate,
            'installment': installment,
            'grade': grade,
            'sub_grade': sub_grade,
            'emp_length': emp_length,
            'home_ownership': home_ownership,
            'annual_inc': annual_inc,
            'issue_date': issue_date.strftime('%Y-%m-%d'),
            'loan_status': status,
            'purpose': purpose,
            'addr_state': addr_state,
            'dti': dti,
            'delinq_2yrs': delinq_2yrs,
            'fico_range_low': fico,
            'fico_range_high': fico + 4,
            'open_acc': open_acc,
            'pub_rec': pub_rec,
            'revol_util': revol_util,
            'total_acc': total_acc,
            'total_pymnt': total_pymnt,
        })
    return rows

if __name__ == '__main__':
    import os
    out_path = os.path.join(os.path.dirname(__file__), 'raw_loans.csv')
    loans = generate_loans(5000)
    with open(out_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=loans[0].keys())
        writer.writeheader()
        writer.writerows(loans)
    print(f"Generated {len(loans)} loan records -> {out_path}")
