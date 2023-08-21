from collections import defaultdict
from flask import Flask, redirect, url_for, render_template
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, IntegerField
from wtforms.validators import DataRequired
from flask_ckeditor import CKEditor
from flask_bootstrap import Bootstrap
import psycopg2
from flask_apscheduler import APScheduler
from flask_sqlalchemy import SQLAlchemy


class Config:
    SCHEDULER_EXECUTORS = {"default": {"type": "threadpool", "max_workers": 20}}
    SCHEDULER_JOB_DEFAULTS = {"coalesce": False, "max_instances": 3}
    SCHEDULER_API_ENABLED = True


app = Flask(__name__)
app.config['SECRET_KEY'] = 'ghholj.vhc@'
ckeditor = CKEditor(app)
Bootstrap(app)
sched = APScheduler()
sched.init_app(app)
data = defaultdict()
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///queries.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


class CreateQueryForm(FlaskForm):
    database_address = StringField("Database Address", validators=[DataRequired()])
    port = IntegerField("Port", validators=[DataRequired()])
    database_name = StringField("Database Name", validators=[DataRequired()])
    user_name = StringField("Username", validators=[DataRequired()])
    password = StringField("Password", validators=[DataRequired()])
    query = StringField("Query", validators=[DataRequired()])
    table_name = StringField("Table Name", validators=[DataRequired()])
    repeat_period = IntegerField("Repeat Period(in hours)", validators=[DataRequired()])
    submit = SubmitField("Submit")


class Query(db.Model):
    id = db.Column(db.Integer, unique=True, primary_key=True)
    database_address = db.Column(db.String(250), nullable=False)
    port = db.Column(db.String(250), nullable=False)
    database_name = db.Column(db.String(250), nullable=False)
    user_name = db.Column(db.String(250), nullable=False)
    password = db.Column(db.String(250), nullable=False)
    query = db.Column(db.String(250), nullable=False)
    table_name = db.Column(db.String(250), nullable=False)
    repeat_period = db.Column(db.Integer, nullable=False)
    result = db.Column(db.String(250))


# with app.app_context():
#     db.create_all()


def query_db():
    with app.app_context():
        first_result = db.session.query(Query).order_by(Query.id.desc()).first()
        data['database_address'] = first_result.database_address
        conn = psycopg2.connect(database=first_result.database_name, user=first_result.user_name,
                                host=first_result.database_address, password=first_result.password,
                                port=first_result.port)
        cur = conn.cursor()
        cur.execute(first_result.query)
        rows = cur.fetchall()
        conn.commit()
        cur.close()
        conn.close()
        result_list = []
        for row in rows:
            result_list.append(row[1])
        result = ', '.join(result_list)
        first_result.result = result
        db.session.commit()
        print('done')


@app.route("/", methods=["GET", "POST"])
def home():
    form = CreateQueryForm()
    if form.validate_on_submit():
        new_query = Query(database_address=form.database_address.data, port=form.port.data,
            database_name=form.database_name.data, user_name=form.user_name.data, password=form.password.data,
            query=form.query.data, table_name=form.table_name.data, repeat_period=form.repeat_period.data,
            result='Undone')
        db.session.add(new_query)
        db.session.commit()
        query_db()
        return redirect(url_for("result"))
    return render_template("home.html", form=form)


@app.route("/result", methods=["GET", "POST"])
def result():
    last_result = db.session.query(Query).order_by(Query.id.desc()).first()
    sched.add_job(id='result', func=query_db, trigger='interval', hours=last_result.repeat_period)
    result_str = last_result.result
    result_list = result_str.split(', ')
    return render_template("query.html", data=result_list)


if __name__ == '__main__':
    sched.start()
    app.config.from_object(Config())
    app.run(debug=True)
