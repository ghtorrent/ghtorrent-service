from flask.ext.wtf import Form
from wtforms import BooleanField, TextField, TextAreaField, validators

class RegistrationForm(Form):
    name = TextField('Name')
    email = TextField('Email Address', [
        validators.Email(),
        validators.Required()
    ])
    requested_projects = TextAreaField('Desired GitHub projects', [validators.Required()])
    accept_tos = BooleanField('I accept the TOS', [validators.Required()])

