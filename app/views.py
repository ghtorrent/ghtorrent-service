from flask import render_template, flash, redirect
from app import app
from forms import RegistrationForm

    
@app.route('/', methods = ['GET', 'POST'])
def register():
#    form = RegistrationForm(request.form)
    form = RegistrationForm()
#    if request.method == 'POST' and form.validate():
#        user = User(form.username.data, form.email.data,
#                    form.password.data)
#        db_session.add(user)
#        flash('Thanks for registering')
#        return redirect(url_for('login'))
    if form.validate_on_submit():
        flash('Thank you, ' + form.name.data + '! Your request was added to the queue. We will notify you at: ' + form.email.data)
        return redirect('/')

    return render_template('register.html', 
        title = 'GHTorrent service',
        form = form)
    
