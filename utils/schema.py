"""
Schema definition for the tables in the database.
"""
from pyspark.sql import types as st

# pylint: disable=invalid-name


companies_schema = st.StructType(
    [
        st.StructField('CompanyId', st.IntegerType(), False),
        st.StructField('CompnayName', st.StringType(), False),
        st.StructField('NumberOfEmployees', st.IntegerType(), False),
        st.StructField('FieldId', st.IntegerType(), False),
        st.StructField('PackageId', st.IntegerType(), False),
        st.StructField('ContactId', st.IntegerType(), True),
    ],
)

contacts_schema = st.StructType(
    [
        st.StructField('ContactId', st.IntegerType(), False),
        st.StructField('Email', st.StringType(), False),
        st.StructField('PhoneNumber', st.StringType(), False),
        st.StructField('Adress', st.StringType(), False),
        st.StructField('Website', st.StringType(), False),
        st.StructField('OwnerId', st.IntegerType(), False),
    ],
)

fields_schema = st.StructType(
    [
        st.StructField('FieldId', st.IntegerType(), False),
        st.StructField('FieldName', st.StringType(), False),
    ],
)

goals_schema = st.StructType(
    [
        st.StructField('GoalId', st.IntegerType(), False),
        st.StructField('GoalName', st.StringType(), False),
        st.StructField('IsCompleted', st.BooleanType(), False),
        st.StructField('CreatedAt', st.DateType(), False),
        st.StructField('FinishedAt', st.DateType(), False),
        st.StructField('MilestoneId', st.IntegerType(), False),
    ],
)

milestones_schema = st.StructType(
    [
        st.StructField('MilestoneId', st.IntegerType(), False),
        st.StructField('MilestoneName', st.StringType(), False),
        st.StructField('CompanyId', st.IntegerType(), False),
        st.StructField('CategoryId', st.IntegerType(), False),
    ],
)

packages_schema = st.StructType(
    [
        st.StructField('PackageId', st.IntegerType(), False),
        st.StructField('PackageName', st.StringType(), False),
        st.StructField('Price', st.IntegerType(), False),
        st.StructField('Description', st.StringType(), False),
    ],
)

products_schema = st.StructType(
    [
        st.StructField('ProductId', st.IntegerType(), False),
        st.StructField('ProductName', st.StringType(), False),
        st.StructField('Description', st.StringType(), False),
        st.StructField('Price', st.IntegerType(), False),
        st.StructField('CompanyId', st.IntegerType(), False),
        st.StructField('CategoryId', st.IntegerType(), False),
    ],
)

reports_schema = st.StructType(
    [
        st.StructField('ReportId', st.IntegerType(), False),
        st.StructField('ReportTitle', st.StringType(), False),
        st.StructField('Content', st.StringType(), False),
        st.StructField('IssuedAt', st.DateType(), False),
        st.StructField('CompanyId', st.IntegerType(), False),
    ],
)

users_schema = st.StructType(
    [
        st.StructField('UserId', st.IntegerType(), False),
        st.StructField('FirstName', st.StringType(), False),
        st.StructField('LastName', st.StringType(), False),
        st.StructField('Email', st.StringType(), False),
        st.StructField('Position', st.StringType(), False),
        st.StructField('CompanyId', st.IntegerType(), False),
    ],
)
