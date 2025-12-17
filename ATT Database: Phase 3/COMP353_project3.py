from typing import List, Optional
from sqlalchemy import ForeignKey, text, case, String, Integer, Float, Boolean, Date, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from datetime import date

engine = create_engine("postgresql+psycopg2://postgres:csclass25@localhost/postgres")

class Base(DeclarativeBase):
    pass


# ============================
#         CUSTOMER
# ============================
class Customer(Base):
    __tablename__ = "customer"

    CustomerID: Mapped[str] = mapped_column(String(6), primary_key=True)
    CustomerFirstName: Mapped[str] = mapped_column(String(50), nullable=False)
    CustomerLastName: Mapped[str] = mapped_column(String(50), nullable=False)
    CustomerEmail: Mapped[str] = mapped_column(String(120), nullable=False, unique=True)
    CustomerPhoneNumber: Mapped[str] = mapped_column(String(20), nullable=False)
    CustomerAddress: Mapped[str] = mapped_column(String(200), nullable=False)

    accounts: Mapped[List["Account"]] = relationship(back_populates="customer")


#Class creations 
class Account(Base):
    __tablename__ = "account"

    AccountID: Mapped[str] = mapped_column(String(6), primary_key=True)
    CustomerID: Mapped[str] = mapped_column(
        String(6),
        ForeignKey("customer.CustomerID"),
        nullable=False
    )
    AccountBalance: Mapped[float] = mapped_column(Float, nullable=False)
    AccountType: Mapped[str] = mapped_column(String(30), nullable=False)
    AccountStatus: Mapped[str] = mapped_column(String(20), nullable=False)
    AccountCreatedDate: Mapped[Date] = mapped_column(Date, nullable=False)

    customer: Mapped["Customer"] = relationship(back_populates="accounts")
    contracts: Mapped[List["Contract"]] = relationship(back_populates="account")
    devices: Mapped[List["Device"]] = relationship(back_populates="account")
    invoices: Mapped[List["Invoice"]] = relationship(back_populates="account")


class Plan(Base):
    __tablename__ = "plan"

    PlanID: Mapped[str] = mapped_column(String(6), primary_key=True)
    PlanName: Mapped[str] = mapped_column(String(20), nullable=False)
    PlanMonthlyFee: Mapped[float] = mapped_column(Float, nullable=False)
    PlanDataLimitGB: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    PlanShareable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    contracts: Mapped[List["Contract"]] = relationship(back_populates="plan")


class Contract(Base):
    __tablename__ = "contract"

    ContractID: Mapped[str] = mapped_column(String(6), primary_key=True)
    ContractStartDate: Mapped[str] = mapped_column(String(30))
    ContractEndDate: Mapped[str] = mapped_column(String(30))
    ContractStatus: Mapped[str] = mapped_column(String(30))

    AccountID: Mapped[str] = mapped_column(
        String(6),
        ForeignKey("account.AccountID"),
        nullable=False
    )
    PlanID: Mapped[str] = mapped_column(
        String(6),
        ForeignKey("plan.PlanID"),
        nullable=False
    )

    account: Mapped["Account"] = relationship(back_populates="contracts")
    plan: Mapped["Plan"] = relationship(back_populates="contracts")


class Device(Base):
    __tablename__ = "device"

    DeviceID: Mapped[str] = mapped_column(String(6), primary_key=True)
    AccountID: Mapped[str] = mapped_column(
        String(6),
        ForeignKey("account.AccountID", ondelete="RESTRICT"),
        nullable=False
    )
    DeviceIMEI: Mapped[str] = mapped_column(String(20), nullable=False, unique=True)
    DeviceModel: Mapped[str] = mapped_column(String(50), nullable=False)

    account: Mapped["Account"] = relationship(back_populates="devices")


class Invoice(Base):
    __tablename__ = "invoice"

    InvoiceID: Mapped[str] = mapped_column(String(6), primary_key=True)
    AccountID: Mapped[str] = mapped_column(
        String(6), ForeignKey("account.AccountID"), nullable=False
    )
    InvoiceDate: Mapped[date] = mapped_column(Date, nullable=False)
    InvoiceDueDate: Mapped[date] = mapped_column(Date, nullable=False)
    InvoiceAmount: Mapped[float] = mapped_column(Float, nullable=False)
    InvoiceStatus: Mapped[str] = mapped_column(String(10), nullable=False)

    account: Mapped["Account"] = relationship(back_populates="invoices")


Base.metadata.create_all(engine)

#Data insertion 


with Session(engine) as session:
        contracts = [
            Contract(
                ContractID = "CT001",
                ContractStartDate= "2023-09-15",
                ContractEndDate = "2025-09-15",
                ContractStatus = "active",
                AccountID = [Account(AccountID = "A001")],
                PlanID = [Plan(PlanID = "P001")]
                ),
            Contract(
                ContractID = "CT002",
                ContractStartDate = "2024-10-10",
                ContractEndDate = "2025-10-10",
                ContractStatus = "active",
                AccountID = [Account(AccountID = "A002")],
                PlanID=[Plan(PlanID = "P002")]
                ),
            Contract(
                ContractID = "CT003",   
                ContractStartDate = "2023-07-10",
                ContractEndDate = "2024-07-10",
                ContractStatus  = "expired",
                AccountID = [Account(AccountID = "A003")],
                PlanID = [Plan(PlanID = "P003")]
                ),
            Contract(
                ContractID = "CT004",
                ContractStartDate = "'2024-03-22",
                ContractEndDate = "2025-03-22",
                ContractStatus  = "active",
                AccountID = [Account(AccountID = "A004")],
                PlanID = [Plan(PlanID = "P001")]
                ),
            Contract(
                ContractID = "CT005",   
                ContractStartDate = "2024-02-22",
                ContractEndDate = "2025-02-22",
                ContractStatus  = "active",
                AccountID = [Account(AccountID = "A005")],
                PlanID = [Plan(PlanID = "P002")]
                ),
            Contract(
                ContractID = "CT006",
                ContractStartDate = "2024-01-18",
                ContractEndDate = "2024-06-18",
                ContractStatus  = "canceled",
                AccountID = [Account(AccountID = "A006")],
                PlanID = [Plan(PlanID = "P003")]
                ),
            Contract(
                ContractID = "CT007",
                ContractStartDate = "2024-03-01",
                ContractEndDate = "2025-03-01",
                ContractStatus = "active",
                AccountID = [Account(AccountID = "A007")],
                PlanID = [Plan(PlanID = "P004")]
            ),
            Contract(
                ContractID = "CT008",
                ContractStartDate = "2024-05-01",
                ContractEndDate = "2025-04-10",
                ContractStatus = "active",
                AccountID = [Account(AccountID = "A008")],
                PlanID = [Plan(PlanID = "P005")]
            ),
            Contract(
                ContractID = "CT009",
                ContractStartDate = "2023-12-15",
                ContractEndDate = "2024-06-15",
                ContractStatus = "expired",
                AccountID = [Account(AccountID = "A009")],
                PlanID = [Plan(PlanID = "P001")]
            ),
            Contract(
                ContractID = "CT010",
                ContractStartDate = "2024-05-01",
                ContractEndDate = "2025-05-01",
                ContractStatus = "active",
                AccountID = [Account(AccountID = "A010")],
                PlanID = [Plan(PlanID = "P002")]
            ), 
            Contract(
                ContractID = "CT011",
                ContractStartDate = "2024-06-10",
                ContractEndDate = "2025-06-10",
                ContractStatus = "active",
                AccountID = [Account(AccountID = "A011")],
                PlanID = [Plan(PlanID = "P003")]        
            ), 
            Contract(
                ContractID = "CT012",
                ContractStartDate = "2024-07-05",
                ContractEndDate = "2025-01-05",
                ContractStatus = "canceled",
                AccountID = [Account(AccountID = "A012")],
                PlanID = [Plan(PlanID = "P004")]        
            )
            ]
        customers = [
            Customer(
                CustomerID="C001",
                CustomerFirstName="Emma",
                CustomerLastName="Johnson",
                CustomerEmail="emma.johnson@email.com",
                CustomerPhoneNumber="3125551234",
                CustomerAddress="123 Oak St, Chicago IL",
            ),
            Customer(
                CustomerID="C002",
                CustomerFirstName="Liam",
                CustomerLastName="Smith",
                CustomerEmail="liam.smith@email.com",
                CustomerPhoneNumber="7735554567",
                CustomerAddress="456 Pine Ave, Evanston IL",
            ),
            Customer(
                CustomerID="C003",
                CustomerFirstName="Ava",
                CustomerLastName="Brown",
                CustomerEmail="ava.brown@email.com",
                CustomerPhoneNumber="8475557890",
                CustomerAddress="789 Maple Dr, Skokie IL",
            ),
            Customer(
                CustomerID="C004",
                CustomerFirstName="Noah",
                CustomerLastName="Davis",
                CustomerEmail="noah.davis@email.com",
                CustomerPhoneNumber="6305552345",
                CustomerAddress="321 Birch Ln, Naperville IL",
            ),
            Customer(
                CustomerID="C005",
                CustomerFirstName="Olivia",
                CustomerLastName="Miller",
                CustomerEmail="olivia.miller@email.com",
                CustomerPhoneNumber="2245556789",
                CustomerAddress="654 Cedar St, Glenview IL",
            ),
            Customer(
                CustomerID="C006",
                CustomerFirstName="Ethan",
                CustomerLastName="Garcia",
                CustomerEmail="ethan.garcia@email.com",
                CustomerPhoneNumber="7085559123",
                CustomerAddress="987 Walnut Rd, Oak Park IL",
            ),
            Customer(
                CustomerID="C007",
                CustomerFirstName="Sophia",
                CustomerLastName="Martinez",
                CustomerEmail="sophia.martinez@email.com",
                CustomerPhoneNumber="3125556789",
                CustomerAddress="245 Elm St, Chicago IL",
            ),
            Customer(
                CustomerID="C008",
                CustomerFirstName="Mason",
                CustomerLastName="Anderson",
                CustomerEmail="mason.anderson@email.com",
                CustomerPhoneNumber="7735553456",
                CustomerAddress="457 Poplar Dr, Des Plaines IL",
            ),
            Customer(
                CustomerID="C009",
                CustomerFirstName="Isabella",
                CustomerLastName="Thomas",
                CustomerEmail="isabella.thomas@email.com",
                CustomerPhoneNumber="8475559988",
                CustomerAddress="122 Spruce Ln, Schaumburg IL",
            ),
            Customer(
                CustomerID="C010",
                CustomerFirstName="James",
                CustomerLastName="White",
                CustomerEmail="james.white@email.com",
                CustomerPhoneNumber="7085557654",
                CustomerAddress="88 Hickory Rd, Oak Lawn IL",
            ),
            Customer(
                CustomerID="C011",
                CustomerFirstName="Mia",
                CustomerLastName="Hernandez",
                CustomerEmail="mia.hernandez@email.com",
                CustomerPhoneNumber="6305553344",
                CustomerAddress="210 Willow Ave, Aurora IL",
            ),
            Customer(
                CustomerID="C012",
                CustomerFirstName="Lucas",
                CustomerLastName="Lopez",
                CustomerEmail="lucas.lopez@email.com",
                CustomerPhoneNumber="2245551122",
                CustomerAddress="512 Aspen Blvd, Palatine IL",
            ),
        ]
        accounts = [
            Account(
                AccountID="A001",
                CustomerID="C001",
                AccountBalance=75.50,
                AccountType="Mobile",
                AccountStatus="active",
                AccountCreatedDate=date(2024, 9, 15),
            ),
            Account(
                AccountID="A002",
                CustomerID="C001",
                AccountBalance=45.25,
                AccountType="Internet",
                AccountStatus="active",
                AccountCreatedDate=date(2024, 10, 10),
            ),
            Account(
                AccountID="A003",
                CustomerID="C002",
                AccountBalance=0.00,
                AccountType="Wireless",
                AccountStatus="inactive",
                AccountCreatedDate=date(2023, 7, 10),
            ),
            Account(
                AccountID="A004",
                CustomerID="C002",
                AccountBalance=90.00,
                AccountType="Mobile",
                AccountStatus="active",
                AccountCreatedDate=date(2024, 3, 22),
            ),
            Account(
                AccountID="A005",
                CustomerID="C003",
                AccountBalance=152.75,
                AccountType="Internet",
                AccountStatus="active",
                AccountCreatedDate=date(2024, 2, 22),
            ),
            Account(
                AccountID="A006",
                CustomerID="C004",
                AccountBalance=60.00,
                AccountType="Wireless",
                AccountStatus="suspended",
                AccountCreatedDate=date(2024, 1, 18),
            ),
            Account(
                AccountID="A007",
                CustomerID="C005",
                AccountBalance=105.00,
                AccountType="Mobile",
                AccountStatus="active",
                AccountCreatedDate=date(2024, 3, 1),
            ),
            Account(
                AccountID="A008",
                CustomerID="C006",
                AccountBalance=85.25,
                AccountType="Internet",
                AccountStatus="active",
                AccountCreatedDate=date(2024, 4, 10),
            ),
            Account(
                AccountID="A009",
                CustomerID="C007",
                AccountBalance=20.50,
                AccountType="Wireless",
                AccountStatus="inactive",
                AccountCreatedDate=date(2023, 12, 15),
            ),
            Account(
                AccountID="A010",
                CustomerID="C008",
                AccountBalance=99.99,
                AccountType="Mobile",
                AccountStatus="active",
                AccountCreatedDate=date(2024, 5, 1),
            ),
            Account(
                AccountID="A011",
                CustomerID="C009",
                AccountBalance=130.00,
                AccountType="Internet",
                AccountStatus="active",
                AccountCreatedDate=date(2024, 6, 10),
            ),
            Account(
                AccountID="A012",
                CustomerID="C003",
                AccountBalance=40.75,
                AccountType="Wireless",
                AccountStatus="active",
                AccountCreatedDate=date(2024, 7, 5),
            ),
        ]
        plans = [
             Plan(
                PlanID="P001",
                PlanName="Unlimited Premium PL",
                PlanMonthlyFee=50.99,
                PlanDataLimitGB=None,
                PlanShareable=True,
            ),
            Plan(
                PlanID="P002",
                PlanName="Unlimited Extra EL",
                PlanMonthlyFee=40.99,
                PlanDataLimitGB=None,
                PlanShareable=True,
            ),
            Plan(
                PlanID="P003",
                PlanName="Unlimited Starter SL",
                PlanMonthlyFee=35.99,
                PlanDataLimitGB=None,
                PlanShareable=True,
            ),
            Plan(
                PlanID="P004",
                PlanName="Value Plus VL",
                PlanMonthlyFee=30.99,
                PlanDataLimitGB=5,
                PlanShareable=True,
            ),
            Plan(
                PlanID="P005",
                PlanName="4GB",
                PlanMonthlyFee=40.00,
                PlanDataLimitGB=4,
                PlanShareable=True,
            ),
            Plan(
                PlanID="P006",
                PlanName="Value Plus VL",
                PlanMonthlyFee=30.99,
                PlanDataLimitGB=5,
                PlanShareable=True,
            ),
            Plan(
                PlanID="P007",
                PlanName="Family Share 10GB",
                PlanMonthlyFee=45.00,
                PlanDataLimitGB=10,
                PlanShareable=True,
            ),
            Plan(
                PlanID="P008",
                PlanName="Business Max",
                PlanMonthlyFee=60.00,
                PlanDataLimitGB=None,
                PlanShareable=True,
            ),
            Plan(
                PlanID="P009",
                PlanName="Student Saver 2GB",
                PlanMonthlyFee=25.00,
                PlanDataLimitGB=2,
                PlanShareable=False,
            ),
            Plan(
                PlanID="P010",
                PlanName="Senior Connect",
                PlanMonthlyFee=28.99,
                PlanDataLimitGB=3,
                PlanShareable=False,
            ),
            Plan(
                PlanID="P011",
                PlanName="Unlimited Enterprise",
                PlanMonthlyFee=75.00,
                PlanDataLimitGB=None,
                PlanShareable=True,
            ),
            Plan(
                PlanID="P012",
                PlanName="Eco Saver",
                PlanMonthlyFee=32.50,
                PlanDataLimitGB=6,
                PlanShareable=True,
            ),
        ]
        devices = [
            Device(DeviceID="D001", AccountID="A001", DeviceIMEI="IMEI100000000001", DeviceModel="iPhone 15 Pro"),
            Device(DeviceID="D002", AccountID="A002", DeviceIMEI="IMEI100000000002", DeviceModel="Samsung Galaxy S24"),
            Device(DeviceID="D003", AccountID="A004", DeviceIMEI="IMEI100000000003", DeviceModel="Google Pixel 8"),
            Device(DeviceID="D004", AccountID="A005", DeviceIMEI="IMEI100000000004", DeviceModel="iPad Air"),
            Device(DeviceID="D005", AccountID="A007", DeviceIMEI="IMEI100000000005", DeviceModel="iPhone 14"),
            Device(DeviceID="D006", AccountID="A008", DeviceIMEI="IMEI100000000006", DeviceModel="Samsung Galaxy Tab S9"),
            Device(DeviceID="D007", AccountID="A010", DeviceIMEI="IMEI100000000007", DeviceModel="Apple Watch Ultra"),
            Device(DeviceID="D008", AccountID="A011", DeviceIMEI="IMEI100000000008", DeviceModel="Samsung Galaxy Z Flip"),
            Device(DeviceID="D009", AccountID="A012", DeviceIMEI="IMEI100000000009", DeviceModel="Motorola Edge 50"),
        ]
        invoices = [
             Invoice(
            InvoiceID="I001",
            AccountID="A001",
            InvoiceDate=date(2024, 9, 20),
            InvoiceDueDate=date(2024, 10, 20),
            InvoiceAmount=75.50,
            InvoiceStatus="paid"
        ),
        Invoice(
            InvoiceID="I002",
            AccountID="A002",
            InvoiceDate=date(2024, 10, 12),
            InvoiceDueDate=date(2024, 11, 12),
            InvoiceAmount=45.25,
            InvoiceStatus="unpaid"
        ),
        Invoice(
            InvoiceID="I003",
            AccountID="A003",
            InvoiceDate=date(2023, 7, 15),
            InvoiceDueDate=date(2023, 8, 15),
            InvoiceAmount=0.00,
            InvoiceStatus="paid"
        ),
        Invoice(
            InvoiceID="I004",
            AccountID="A004",
            InvoiceDate=date(2024, 3, 25),
            InvoiceDueDate=date(2024, 4, 25),
            InvoiceAmount=90.00,
            InvoiceStatus="paid"
        ),
        Invoice(
            InvoiceID="I005",
            AccountID="A005",
            InvoiceDate=date(2024, 2, 28),
            InvoiceDueDate=date(2024, 3, 28),
            InvoiceAmount=152.75,
            InvoiceStatus="unpaid"
        ),
        Invoice(
            InvoiceID="I006",
            AccountID="A006",
            InvoiceDate=date(2024, 1, 20),
            InvoiceDueDate=date(2024, 2, 20),
            InvoiceAmount=60.00,
            InvoiceStatus="canceled"
        ),
        Invoice(
            InvoiceID="I007",
            AccountID="A007",
            InvoiceDate=date(2024, 3, 5),
            InvoiceDueDate=date(2024, 4, 5),
            InvoiceAmount=105.00,
            InvoiceStatus="paid"
        ),
        Invoice(
            InvoiceID="I008",
            AccountID="A008",
            InvoiceDate=date(2024, 4, 15),
            InvoiceDueDate=date(2024, 5, 15),
            InvoiceAmount=85.25,
            InvoiceStatus="unpaid"
        ),
        Invoice(
            InvoiceID="I009",
            AccountID="A009",
            InvoiceDate=date(2023, 12, 18),
            InvoiceDueDate=date(2024, 1, 18),
            InvoiceAmount=20.50,
            InvoiceStatus="overdue"
        ),
        Invoice(
            InvoiceID="I010",
            AccountID="A010",
            InvoiceDate=date(2024, 5, 2),
            InvoiceDueDate=date(2024, 6, 2),
            InvoiceAmount=99.99,
            InvoiceStatus="paid"
        ),
        Invoice(
            InvoiceID="I011",
            AccountID="A011",
            InvoiceDate=date(2024, 6, 12),
            InvoiceDueDate=date(2024, 7, 12),
            InvoiceAmount=130.00,
            InvoiceStatus="unpaid"
        ),
        Invoice(
            InvoiceID="I012",
            AccountID="A012",
            InvoiceDate=date(2024, 7, 7),
            InvoiceDueDate=date(2024, 8, 7),
            InvoiceAmount=40.75,
            InvoiceStatus="paid"
        )
    ]

"""
session.add_all(customers)
session.add_all(accounts)
session.add_all(contracts)
session.add_all(plans)
session.add_all(devices)
session.commit()
print("Data inserted successfully.")
"""

#Queries

stmt = (
    select(
        Customer.CustomerID,
        Customer.CustomerFirstName,
        Customer.CustomerLastName,
        Customer.CustomerEmail,
    )
    .join(Account)
    .join(Contract)
    .where(Contract.ContractStatus == "active")
)

print("Query 1:")
with Session(engine) as session:
    results = session.execute(stmt).all()
    for row in results:
        print(f"CustomerID: {row.CustomerID}, FirstName: {row.CustomerFirstName}, LastName: {row.CustomerLastName}, Email: {row.CustomerEmail}")

stmt = (
    select(
        Customer.CustomerID,
        Customer.CustomerFirstName,
        Customer.CustomerLastName,
        Customer.CustomerEmail,
        Account.AccountID,
        Account.AccountType,
        Account.AccountStatus,
        Account.AccountBalance,
    )
    .join(Account)
    .where(Account.AccountStatus == "active")
    .order_by(Account.AccountBalance.desc())
    .limit(15)
)

results = session.execute(stmt).all()
print("\nQuery 2: Top 15 Customers with Active Accounts by Balance:")
for row in results:
    print(
        row.CustomerID,
        row.CustomerFirstName,
        row.CustomerLastName,
        row.CustomerEmail,
        row.AccountID,
        row.AccountType,
        row.AccountStatus,
        row.AccountBalance,
    )

print("\nQuery 3:")

stmt = (
    select(
        Plan.PlanName,
        Plan.PlanMonthlyFee,
        Contract.ContractStatus,
        Account.AccountBalance,
    )
    .join(Contract, Plan.PlanID == Contract.PlanID)
    .join(Account, Contract.AccountID == Account.AccountID)
    .where(
        Contract.ContractStatus == "active",
        Account.AccountBalance < Plan.PlanMonthlyFee,
    )
    .order_by(Account.AccountBalance.desc())
)

results = session.execute(stmt).all()
for row in results:
        print(
            row.PlanName,
            row.PlanMonthlyFee,
            row.ContractStatus,
            row.AccountBalance
        )
    

print("\nQuery 4:")

"""
numDevices = func.count(Device.DeviceID)
numContracts = func.count(func.distinct(Contract.ContractID))

stmt = (
    select(
        Customer.CustomerID,
        Customer.CustomerFirstName,
        Customer.CustomerLastName,
        Account.AccountID,
        numDevices.label("NumDevices"),
        numContracts.label("NumActiveContracts"),
    )
    .join(Account)
    .join(Device)
    .join(Contract)
    .where(
        Account.AccountStatus == "active",
        Contract.ContractStatus == "active",
    )
    .group_by(
        Customer.CustomerID,
        Customer.CustomerFirstName,
        Customer.CustomerLastName,
        Account.AccountID,
    )
    .order_by(numDevices.desc(), numContracts.desc())
)

results = session.execute(stmt).all()

print("\nactiveDevicesSummary Output:\n")
print("CustomerID  CustomerFirstName  CustomerLastName  AccountID  NumDevices  NumActiveContracts")

for row in results:
    print(
        f"{row[0]:<12}{row[1]:<20}"
        f"{row[2]:<18}{row[3]:<12}"
        f"{row[4]:<12}{row[5]}"
    )
"""
print("\nQuery 5:")

"""

stmt = (
    select(
        Account.AccountID,
        func.sum(Invoice.InvoiceAmount).label("TotalInvoiceAmount"),
        func.sum(case((Invoice.InvoiceStatus == "paid", Invoice.InvoiceAmount), else_=0)).label("TotalPaidAmount"),
        func.sum(case((Invoice.InvoiceStatus == "unpaid", Invoice.InvoiceAmount), else_=0)).label("TotalUnpaidAmount"),
        func.sum(case((Invoice.InvoiceStatus == "overdue", 1), else_=0)).label("NumOverdueInvoices"),
    )
    .join(Invoice)
    .group_by(Account.AccountID)
    .order_by(text("TotalUnpaidAmount DESC"))
)

results = session.execute(stmt).all()

print("\n=== Invoice Payment Summary by Account ===")
print(f"{'Account ID':<15} {'Total Invoices':<15} {'Total Paid':<15} {'Total Unpaid':<15} {'Overdue Count':<15}")
print("-" * 80)

for row in results:
        print(
            f"{row.AccountID:<15} "
            f"${row.TotalInvoiceAmount:<14.2f} "
            f"${row.TotalPaidAmount:<14.2f} "
            f"${row.TotalUnpaidAmount:<14.2f} "
            f"{row.NumOverdueInvoices:<15}"
        )

print(f"\nTotal accounts found: {len(results)}")
"""
