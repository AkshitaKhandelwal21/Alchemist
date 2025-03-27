import datetime
import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, TIMESTAMP, Float, VARCHAR, func, distinct
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from flask import Flask, request, jsonify, render_template
from sqlalchemy.inspection import inspect
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

app = Flask(__name__)

engine = create_engine("sqlite:///database.db")
Base = declarative_base()
Session = sessionmaker(bind=engine)

class User(Base):
    __tablename__ = 'sales'

    id = Column(Integer, primary_key=True)
    Date = Column(TIMESTAMP, default=datetime.datetime.now())
    FieldofActivity = Column(String)
    DoctorName = Column(String(100), nullable=False)
    Country = Column(VARCHAR(100), nullable=False)
    States = Column(VARCHAR(100), nullable=False)
    Application = Column(VARCHAR(100), nullable=False)
    ProductName = Column(VARCHAR(100), nullable=False)
    Sales = Column(Float, nullable=False)
    Quantity = Column(Integer, nullable=True)
    Discount = Column(Float)
    Profit = Column(Float, nullable=False)

Base.metadata.create_all(engine)

@app.route('/users', methods=['GET', 'POST', 'PATCH', 'DELETE'])
def create_user():
    session = Session()
    try:
        if request.method == 'POST':
            data = request.get_json()
            mapper = inspect(User)
            columns = [column.key for column in mapper.attrs]

            new_users = []
            for user_data in data:
                new_user_data = {column: user_data[column] for column in columns if column in user_data}
                new_user = User(**new_user_data)
                session.add(new_user)
                new_users.append(new_user)

            session.commit()
            return jsonify({"message": f"{len(new_users)} users created successfully!"}), 201

        elif request.method == 'GET':
            mapper = inspect(User)
            columns = [column.key for column in mapper.attrs]
            filters = {col: request.args.get(col) for col in columns if request.args.get(col)}

            query = session.query(User)
            for column, value in filters.items():
                query = query.filter(getattr(User, column).like(f"%{value}%"))

            users = query.all()
            output = [{col: getattr(user, col) for col in columns} for user in users]
            return jsonify({'users': output})

        elif request.method == 'PATCH':
            query_params = request.args
            mapper = inspect(User)
            columns = [column.key for column in mapper.attrs]
            query = session.query(User)

            for param, value in query_params.items():
                if param in columns:
                    query = query.filter(getattr(User, param).like(f"%{value}%"))

            users = query.all()
            if not users:
                return jsonify({"message": "User(s) not found!"}), 404

            data = request.get_json()
            for user in users:
                for column in columns:
                    if column in data:
                        setattr(user, column, data[column])

            session.commit()
            return jsonify({"message": f"{len(users)} user(s) updated successfully!"}), 200

        elif request.method == 'DELETE':
            query_params = request.args
            mapper = inspect(User)
            columns = [column.key for column in mapper.attrs]
            query = session.query(User)

            for param, value in query_params.items():
                if param in columns:
                    query = query.filter(getattr(User, param).like(f"%{value}%"))

            users = query.all()
            if not users:
                return jsonify({"message": "User(s) not found!"}), 404

            for user in users:
                session.delete(user)

            session.commit()
            return jsonify({"message": f"{len(users)} user(s) deleted successfully!"}), 200

    except Exception as e:
        session.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()

def fetch_data():
    session = Session()
    try:
        mapper = inspect(User)
        columns = [column.key for column in mapper.attrs]
        users = session.query(User).all()

        output = [{column: getattr(user, column) for column in columns} for user in users]
        df = pd.DataFrame(output)

        print("Fetched DataFrame Columns:", df.columns) 

        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()
    finally:
        session.close()

def generate_chart(df):
    required_columns = ['Application', 'Sales', 'Profit', 'Quantity']
    missing_cols = [col for col in required_columns if col not in df.columns]

    if missing_cols:
        print(f"Missing columns in DataFrame: {missing_cols}")
        return None, None, None, None, None  # Return None if data is incomplete

    grouped_data1 = df.groupby('Application').agg({'Sales': 'mean', 'Profit': 'mean', 'Quantity': 'mean'})
    fig1 = go.Figure(data=[
        go.Bar(name='Sales', x=grouped_data1.index, y=grouped_data1['Sales']),
        go.Bar(name='Profit', x=grouped_data1.index, y=grouped_data1['Profit']),
        go.Bar(name='Quantity', x=grouped_data1.index, y=grouped_data1['Quantity'])
    ])
    fig1.update_layout(title='Average Sales, Profit, and Quantity by Application')

    sales_by_field = df.groupby('FieldofActivity')['Sales'].sum().reset_index()
    fig2 = px.pie(sales_by_field, values='Sales', names='FieldofActivity', title='Percent Sales by Field of Activity')

    doctor_sales = df.groupby('DoctorName')['Sales'].mean().reset_index().sort_values('Sales', ascending=False).head(5)
    fig3 = px.bar(doctor_sales, x='Sales', y='DoctorName', orientation='h', title='Top 5 Doctors by Sales')

    fig4 = px.scatter(df, x='Discount', y='Profit', size='Quantity', color='ProductName', title='Profit vs. Quantity')

    grouped_data5 = df.groupby(['FieldofActivity', 'States'])['Sales'].mean().unstack()
    fig5 = go.Figure()
    for state in grouped_data5.columns:
        fig5.add_trace(go.Scatter(x=grouped_data5.index, y=grouped_data5[state], stackgroup='one', name=state))
    fig5.update_layout(title='Sales by States')

    return fig2.to_html(full_html=False), fig5.to_html(full_html=False), fig3.to_html(full_html=False), fig4.to_html(full_html=False), fig1.to_html(full_html=False)

@app.route('/')
def dashboard():
    df = fetch_data()
    fig2, fig5, fig3, fig4, fig1 = generate_chart(df) if not df.empty else (None, None, None, None, None)

    session = Session()
    num_doctors = session.query(func.count(distinct(User.DoctorName))).scalar()
    num_products = session.query(func.count(distinct(User.ProductName))).scalar()
    avg_sales = round(session.query(func.sum(User.Sales)).scalar() or 0, 2)
    avg_profit = round(session.query(func.avg(User.Profit)).scalar() or 0, 2)
    session.close()

    return render_template('home.html', fig2=fig2, fig5=fig5, fig3=fig3, fig4=fig4, fig1=fig1, num_doctors=num_doctors, num_products=num_products, avg_sales=avg_sales, avg_profit=avg_profit)

if __name__ == '__main__':
    app.run(port=8080, debug=True)
