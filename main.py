import os
import sqlite3

import pandas as pd


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data.sqlite")


def _connect() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


conn = _connect()


df_boston = pd.read_sql(
    """
    SELECT e.firstName, e.lastName, e.jobTitle
    FROM employees AS e
    JOIN offices AS o
        ON e.officeCode = o.officeCode
    WHERE o.city = 'Boston';
    """,
    conn,
)


df_zero_emp = pd.read_sql(
    """
    SELECT o.officeCode, o.city
    FROM offices AS o
    LEFT JOIN employees AS e
        ON o.officeCode = e.officeCode
    WHERE e.employeeNumber IS NULL;
    """,
    conn,
)


df_employee = pd.read_sql(
    """
    SELECT e.firstName, e.lastName, o.city, o.state
    FROM employees AS e
    LEFT JOIN offices AS o
        ON e.officeCode = o.officeCode
    ORDER BY e.firstName ASC, e.lastName ASC;
    """,
    conn,
)


df_contacts = pd.read_sql(
    """
    SELECT c.contactFirstName, c.contactLastName, c.phone, c.salesRepEmployeeNumber
    FROM customers AS c
    LEFT JOIN orders AS o
        ON c.customerNumber = o.customerNumber
    WHERE o.customerNumber IS NULL
    ORDER BY c.contactLastName ASC;
    """,
    conn,
)


df_payment = pd.read_sql(
    """
    SELECT c.contactFirstName, c.contactLastName, p.amount, p.paymentDate
    FROM customers AS c
    JOIN payments AS p
        ON c.customerNumber = p.customerNumber
    ORDER BY CAST(p.amount AS REAL) DESC;
    """,
    conn,
)


df_credit = pd.read_sql(
    """
    SELECT e.employeeNumber, e.firstName, e.lastName, COUNT(c.customerNumber) AS numberOfCustomers
    FROM employees AS e
    JOIN customers AS c
        ON e.employeeNumber = c.salesRepEmployeeNumber
    GROUP BY e.employeeNumber, e.firstName, e.lastName
    HAVING AVG(c.creditLimit) > 90000
    ORDER BY numberOfCustomers DESC
    LIMIT 4;
    """,
    conn,
)


df_product_sold = pd.read_sql(
    """
    SELECT p.productName, COUNT(o.orderNumber) AS numorders, SUM(od.quantityOrdered) AS totalunits
    FROM products AS p
    JOIN orderdetails AS od
        ON p.productCode = od.productCode
    JOIN orders AS o
        ON od.orderNumber = o.orderNumber
    GROUP BY p.productName
    ORDER BY totalunits DESC;
    """,
    conn,
)


df_total_customers = pd.read_sql(
    """
    SELECT p.productName, p.productCode, COUNT(DISTINCT c.customerNumber) AS numpurchasers
    FROM products AS p
    JOIN orderdetails AS od
        ON p.productCode = od.productCode
    JOIN orders AS o
        ON od.orderNumber = o.orderNumber
    JOIN customers AS c
        ON o.customerNumber = c.customerNumber
    GROUP BY p.productName, p.productCode
    ORDER BY numpurchasers DESC;
    """,
    conn,
)


df_customers = pd.read_sql(
    """
    SELECT o.officeCode, o.city, COUNT(c.customerNumber) AS n_customers
    FROM offices AS o
    LEFT JOIN employees AS e
        ON o.officeCode = e.officeCode
    LEFT JOIN customers AS c
        ON e.employeeNumber = c.salesRepEmployeeNumber
    GROUP BY o.officeCode, o.city;
    """,
    conn,
)


df_under_20 = pd.read_sql(
    """
    SELECT DISTINCT e.employeeNumber, e.firstName, e.lastName, off.city, off.officeCode
    FROM employees AS e
    JOIN offices AS off
        ON e.officeCode = off.officeCode
    JOIN customers AS c
        ON e.employeeNumber = c.salesRepEmployeeNumber
    JOIN orders AS ord
        ON c.customerNumber = ord.customerNumber
    JOIN orderdetails AS od
        ON ord.orderNumber = od.orderNumber
    JOIN (
        SELECT od_inner.productCode
        FROM orderdetails AS od_inner
        JOIN orders AS o_inner
            ON od_inner.orderNumber = o_inner.orderNumber
        GROUP BY od_inner.productCode
        HAVING COUNT(DISTINCT o_inner.customerNumber) < 20
    ) AS pu20
        ON od.productCode = pu20.productCode;""",
    conn,
)


conn.close()
