from urllib import request
from flask import Flask, render_template, redirect, url_for, flash
from flask_sqlalchemy import SQLAlchemy
from flask_wtf import FlaskForm
from wtforms import StringField, TextAreaField, SubmitField
from wtforms.validators import DataRequired

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///blog.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = 'your_secret_key'
db = SQLAlchemy(app)


class Post(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(100), nullable=False)
    content = db.Column(db.Text, nullable=False)


class PostForm(FlaskForm):
    title = StringField('Title', validators=[DataRequired()])
    content = TextAreaField('Content', validators=[DataRequired()])
    submit = SubmitField('Submit')


@app.route('/')
def home():
    return redirect('/posts')


@app.route('/posts')
def posts():
    all_posts = Post.query.all()
    return render_template('posts.html', posts=all_posts)


@app.route('/post/<int:post_id>')
def post(post_id):
    single_post = Post.query.get_or_404(post_id)
    return render_template('post.html', post=single_post)


@app.route('/new', methods=['GET', 'POST'])
def new_post():
    form = PostForm()
    if form.validate_on_submit():
        new_post = Post(title=form.title.data, content=form.content.data)
        db.session.add(new_post)
        db.session.commit()
        flash('Post created successfully!', 'success')
        return redirect(url_for('posts'))
    return render_template('form.html', form=form, title="Create a New Post")


@app.route('/edit/<int:post_id>', methods=['GET', 'POST'])
def edit_post(post_id):
    post = Post.query.get_or_404(post_id)
    form = PostForm()
    if form.validate_on_submit():
        post.title = form.title.data
        post.content = form.content.data
        db.session.commit()
        flash('Post updated successfully!', 'success')
        return redirect(url_for('posts'))
    elif request.method == 'GET':
        form.title.data = post.title
        form.content.data = post.content
    return render_template('form.html', form=form, title="Edit Post")


@app.route('/delete/<int:post_id>')
def delete_post(post_id):
    post = Post.query.get_or_404(post_id)
    db.session.delete(post)
    db.session.commit()
    flash('Post deleted successfully!', 'success')
    return redirect(url_for('posts'))


with app.app_context():
    db.create_all()
    if Post.query.count() == 0:
        db.session.add(Post(title="Sample Post 1",
                       content="This is a sample post."))
        db.session.add(Post(title="Sample Post 2",
                       content="Another sample post."))
        db.session.commit()

if __name__ == '__main__':
    app.run(debug=True)
