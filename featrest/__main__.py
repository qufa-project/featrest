from . import app as flask_application


if __name__ == '__main__':
    flask_application.app.run(host='0.0.0.0')
