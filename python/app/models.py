from app import db


class User(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    name = db.Column(db.String(64), index = True)
    email = db.Column(db.String(120), index = True, unique = True)
    requests = db.relationship('Request', backref = 'author', lazy = 'dynamic')

    def __repr__(self):
        return '<User %r>' % (self.email)
    
    
class Project(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    url = db.Column(db.String(255), index = True)
    last_updated = db.Column(db.DateTime)
    requests = db.relationship('Request', backref = 'requested_by', lazy = 'dynamic')
      
    def __repr__(self):
        return '<Project %r, last updated on: %r>' % (self.url, self.last_updated)
        

class Request(db.Model):
    id = db.Column(db.Integer, primary_key = True)
#    body = db.Column(db.String(140))
    timestamp = db.Column(db.DateTime)
    done = db.Column(db.Boolean)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    project_id = db.Column(db.Integer, db.ForeignKey('project.id'))
    
    def __repr__(self):
        return '<Request %r at %r by %r for project %r. Done: %r>' % (self.id, self.timestamp, self.user_id, self.project_id, self.done)
    
