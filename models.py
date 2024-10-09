import datetime
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, TIMESTAMP, Float, VARCHAR, func, distinct
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from flask import Flask, request, jsonify, render_template
from sqlalchemy.inspection import inspect
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
import plotly.graph_objects as go


app = Flask(__name__)

engine = create_engine("sqlite:///database.db")
Base = declarative_base()
Session = sessionmaker(bind=engine)
# session = Session()


# class Users(Base):
#     __tablename__ = 'users'
#
#     id = Column(Integer, primary_key=True)
#     name = Column(String(50), nullable=False)
#     email = Column(String(100), nullable=False)
#     age = Column(Integer, nullable=True)


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


# @app.route('/')
# def index():
#     session = Session()
#     session.close()
#     return "Hello, World!"


@app.route('/users', methods=['GET', 'POST', 'PATCH', 'DELETE'])
def create_user():
    if request.method == 'POST':
        session = Session()
        try:
            data = request.get_json()
            mapper = inspect(User)
            columns = [column.key for column in mapper.attrs]
            arr = []
            for user_data in data:
                new_user_data = {column: user_data[column] for column in columns if column in user_data != User.id}
                new_user = User(**new_user_data)
                session.add(new_user)
                arr.append(new_user)
            session.commit()
            return jsonify({"message": f"{len(arr)} users created successfully!"}), 201

        except Exception as e:
            session.rollback()
            return jsonify("Cannot create user: " + str(e)), 500
        finally:
            session.close()


# @app.route('/users', methods=['GET'])
# def get_users():
    elif(request.method == 'GET'):
        session = Session()
        try:
            mapper = inspect(User)
            columns = [column.key for column in mapper.attrs]
            filters = {}
            for column in columns:
                value = request.args.get(column)
                if value:
                    filters[column] = value
            query = session.query(User)
            for column, value in filters.items():
                query = query.filter(getattr(User, column).like(f"%{value}%"))
            users = query.all()
            output = []
            for user in users:
                user_data = {column: getattr(user, column) for column in columns}
                output.append(user_data)
            return jsonify({'users': output})

        except Exception as e:
            return jsonify({"error": str(e)}), 500
        finally:
            session.close()


# @app.route('/users/<int:id>', methods=['PUT'])
# def update_user(id):
#     session = Session()
#     try:
#         user = session.query(User).get(id)
#         if not user:
#             return jsonify({"message": "User not found!"}), 404
#         data = request.get_json()
#         mapper = inspect(User)
#         columns = [column.key for column in mapper.attrs]
#         for column in columns:
#             if column in data:
#                 setattr(user, column, data[column])
#         session.commit()
#         return jsonify({"message": "User updated successfully!"}), 200
#     except Exception as e:
#         session.rollback()
#         return jsonify({"error": str(e)}), 500
#     finally:
#         session.close()


# @app.route('/users', methods=['PATCH'])
# def update_user_details():
    elif(request.method == 'PATCH'):
        session = Session()
        try:
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

        except Exception as e:
            session.rollback()
            return jsonify({"error": str(e)}), 500
        finally:
            session.close()


# @app.route('/users', methods=['DELETE'])
# def delete_user():
    elif(request.method == 'DELETE'):
        session = Session()
        try:
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


# Here is the visualization code

def fetch_data():
    session = Session()
    try:
        mapper = inspect(User)
        columns = [column.key for column in mapper.attrs]
        users = session.query(User).all()

        output = []
        for user in users:
            user_data = {column: getattr(user, column) for column in columns}
            output.append(user_data)
        df = pd.DataFrame(output)
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
    finally:
        session.close()


# def process_data(df):
#     df['domain'] = df['email'].apply(lambda x: x.split('@')[-1])
#     domain_count = df.groupby('domain').size().reset_index(name='count')
#     return domain_count



def generate_chart(df):

    # Stacked bar chart for Average Sales, Profit, and Quantity by Field of Activity
    grouped_data1 = df.groupby('Application').agg({'Sales': 'mean', 'Profit': 'mean', 'Quantity': 'mean'})
    fig1 = go.Figure(data=[
        go.Bar(name='Sales', x=grouped_data1.index, y=grouped_data1['Sales']),
        go.Bar(name='Profit', x=grouped_data1.index, y=grouped_data1['Profit']),
        go.Bar(name='Quantity', x=grouped_data1.index, y=grouped_data1['Quantity'])
    ])
    # fig1.update_layout(barmode='stack', title='Average Sales, Profit, and Quantity by Application', xaxis_title='Application', yaxis_title='Average Value')
    fig1.update_layout(
        title='Average Sales, Profit, and Quantity by Application',
        # title_font=dict(size=20),
        xaxis_title='Application',
        yaxis_title='Average Value',
        margin=dict(l=40, r=40, t=40, b=40),
        paper_bgcolor='white', plot_bgcolor='white',
        font=dict(color="black")
    )
    fig1.update_xaxes(gridcolor='grey')
    fig1.update_yaxes(gridcolor='grey')



    # Group the data by 'FieldOfActivity' and calculate the mean of 'Sales', 'Profit', and 'Quantity'
    # grouped_data2 = df.groupby('FieldofActivity').agg({'Sales': 'mean', 'Profit': 'mean', 'Quantity': 'mean'}).reset_index()
    # melted_data = pd.melt(grouped_data2, id_vars='FieldofActivity', value_vars=['Sales', 'Profit', 'Quantity'])
    # fig2 = px.bar(melted_data, x='FieldofActivity', y='value', color='variable', barmode='group', title='Average Sales, Profit, and Quantity by Field of Activity')
    # fig2.update_layout(xaxis_title='Field of Activity', yaxis_title='Average Value')


    # Create the pie chart using plotly express
    sales_by_field = df.groupby('FieldofActivity')['Sales'].sum().reset_index()
    fig2 = px.pie(sales_by_field, values='Sales', names='FieldofActivity', title='Percent Sales by Field of Activity',
                 hole=0.3, labels={'Sales': 'Total Sales'})
    fig2.update_traces(textposition='inside', textinfo='percent')
    fig2.update_layout(paper_bgcolor='white', plot_bgcolor='white', font=dict(color="black"))
    fig2.update_xaxes(gridcolor='grey')
    fig2.update_yaxes(gridcolor='grey')



    # Group data by doctor and calculate average sales
    doctor_sales = df.groupby('DoctorName')['Sales'].mean().reset_index()
    doctor_sales = doctor_sales.sort_values('Sales', ascending=False).head(5)
    fig3 = px.bar(doctor_sales, x='Sales', y='DoctorName', orientation='h', title='Ranking of Doctors based on Average Sales')
    fig3.update_layout(xaxis_title='Average Sales', yaxis_title='Doctor Name', paper_bgcolor='white', plot_bgcolor='white', font=dict(color="black"))
    fig3.update_xaxes(gridcolor='grey')
    fig3.update_yaxes(gridcolor='grey')




    # Bubble chart for profit by discount of products
    fig4 = px.scatter(df, x='Discount', y='Profit', size='Quantity', color='ProductName',
                     hover_name='ProductName', title='Profit vs. Quantity with Discount as Bubble Size')
    fig4.update_layout(xaxis_title='Quantity', yaxis_title='Profit', paper_bgcolor='white', plot_bgcolor='white', font=dict(color="black"))
    fig4.update_xaxes(gridcolor='grey')
    fig4.update_yaxes(gridcolor='grey')




    # Create the stacked area chart using plotly graph objects
    grouped_data5 = df.groupby(['FieldofActivity', 'States'])['Sales'].mean().unstack()
    state_order = grouped_data5.sum().sort_values(ascending=True).index
    fig5 = go.Figure()
    for state in state_order:
        fig5.add_trace(go.Scatter(
            x=grouped_data5.index,
            y=grouped_data5[state],
            stackgroup='one',
            name=state,
            mode='lines',
            fill='tonexty'
        ))
    fig5.update_layout(title='Sales by States', paper_bgcolor='white', plot_bgcolor='white', font=dict(color="black"))
    fig5.update_xaxes(gridcolor='grey')
    fig5.update_yaxes(gridcolor='grey')



    return fig2.to_html(full_html=False), fig5.to_html(full_html=False), fig3.to_html(full_html=False), fig4.to_html(full_html=False), fig1.to_html(full_html=False)
    # return fig2.to_html(full_html=False) + fig5.to_html(full_html=False) + fig3.to_html(full_html=False) + fig4.to_html(full_html=False) + fig1.to_html(full_html=False)


@app.route('/')
def dashboard():
    df = fetch_data()
    session = Session()
    # domain_count_df = process_data(df)
    fig2, fig5, fig3, fig4, fig1 = generate_chart(df)
    # chart_html = generate_chart(df)

    num_doctors = session.query(func.count(distinct(User.DoctorName))).scalar()
    num_products = session.query(func.count(distinct(User.ProductName))).scalar()
    avg_sales = round(session.query(func.sum(User.Sales)).scalar(), 2)
    avg_profit = round(session.query(func.avg(User.Profit)).scalar(), 2)

    return render_template('home.html', fig2=fig2, fig5=fig5, fig3=fig3, fig4=fig4, fig1=fig1, num_doctors=num_doctors, num_products=num_products, avg_sales=avg_sales, avg_profit=avg_profit)



if __name__ == '__main__':
    app.run(port=8080, debug=True)
