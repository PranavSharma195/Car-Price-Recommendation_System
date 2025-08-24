from django.http import HttpResponse
from django import forms
from django.db import connection
from django.conf import settings
from django.core.wsgi import get_wsgi_application

import os
from dotenv import load_dotenv

load_dotenv()

settings.configure(
    DEBUG=True,
    SECRET_KEY=os.getenv("DJANGO_SECRET_KEY", "fallback-key"),
    ROOT_URLCONF=__name__,
    ALLOWED_HOSTS=["*"],
    DATABASES={
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": os.getenv("DB_NAME", "postgres"),
            "USER": os.getenv("DB_USER", "postgres"),
            "PASSWORD": os.getenv("DB_PASSWORD", ""),
            "HOST": os.getenv("DB_HOST", "localhost"),
            "PORT": os.getenv("DB_PORT", "5432"),
        }
    },

    INSTALLED_APPS=[
        "django.contrib.contenttypes",
        "django.contrib.auth",
    ],
    TEMPLATES=[{
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
    }],
    STATIC_URL="/static/",
    STATICFILES_DIRS=["./static"],
)

import django
django.setup()

# ================= FORM =================
class PredictionForm(forms.Form):
    mileage = forms.FloatField(label="Mileage", widget=forms.NumberInput(attrs={"placeholder": "Enter mileage"}))
    msrp = forms.FloatField(label="MSRP (USD)", widget=forms.NumberInput(attrs={"placeholder": "Enter MSRP"}))
    days_on_market = forms.IntegerField(label="Days on Market", min_value=0, widget=forms.NumberInput(attrs={"placeholder": "Enter days on market"}))
    ask_price = forms.FloatField(label="Ask Price (USD)", widget=forms.NumberInput(attrs={"placeholder": "Enter ask price"}))

# ================= HTML TEMPLATES =================
def landing_page(form):
    return f"""
    <html>
    <head>
        <title>Car Price Predictor</title>
        <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@400;700&display=swap" rel="stylesheet">
        <style>
            body {{
                font-family: 'Poppins', sans-serif;
                margin: 0;
                padding: 0;
                height: 100vh;
                display: flex;
                justify-content: center;
                align-items: flex-start; /* align at top instead of exact center */
                background: url('/static/cars.jpg') no-repeat center center fixed;
                background-size: cover;
                padding-top: 60px; /* push box slightly below top */
            }}
            .container {{
                position: relative;
                background: rgba(255, 255, 255, 0.85);
                padding: 40px 50px;
                border-radius: 20px;
                box-shadow: 0 12px 40px rgba(0,0,0,0.3);
                width: 400px;
                text-align: left;
            }}
            h1 {{
                text-align: center;
                margin-bottom: 25px;
                font-size: 28px;
                color: #2C3E50;
            }}
            label {{
                font-weight: 600;
                margin-top: 10px;
                display: block;
            }}
            input {{
                width: 100%;
                padding: 12px;
                margin: 8px 0 16px 0;
                border-radius: 8px;
                border: 1px solid #ccc;
                font-size: 15px;
            }}
            button {{
                width: 100%;
                padding: 14px;
                margin-top: 10px;
                border: none;
                border-radius: 10px;
                font-size: 18px;
                cursor: pointer;
                transition: 0.3s;
                font-weight: bold;
            }}
            .predict-btn {{
                background: linear-gradient(90deg, #ff416c, #ff4b2b);
                color: white;
            }}
            .predict-btn:hover {{
                opacity: 0.9;
            }}
            .clear-btn {{
                background: #6c757d;
                color: white;
            }}
            .clear-btn:hover {{
                background: #5a6268;
            }}
        </style>
        <script>
            function clearForm() {{
                document.querySelector("form").reset();
            }}
        </script>
    </head>
    <body>
        <div class="container">
            <h1>Car Price Predictor</h1>
            <form method="POST" action="/predict">
                {form}
                <button type="submit" class="predict-btn">Predict</button>
                <button type="button" onclick="clearForm()" class="clear-btn">Clear</button>
            </form>
        </div>
    </body>
    </html>
    """


def result_page(pred, diff):
    return f"""
    <html>
    <head>
        <title>Prediction Result</title>
        <style>
            body {{
                font-family: 'Poppins', sans-serif;
                height: 100vh;
                margin: 0;
                background: url('/static/car1.jpg') no-repeat center center fixed;
                background-size: cover;
                position: relative;
            }}
            .container {{
                position: absolute;
                top: 420px;
                right: 80px;
                background: linear-gradient(135deg, rgba(255,255,255,0.15), rgba(255,255,255,0.05));
                padding: 60px 50px;
                border-radius: 25px;
                width: 520px;
                text-align: center;
                backdrop-filter: blur(18px);
                border: 1px solid rgba(255,255,255,0.4);
                box-shadow: 0 8px 40px rgba(0,0,0,0.5);
                color: #fff;
                overflow: hidden;
            }}
            .container::before {{
                content: "";
                position: absolute;
                top: -50%;
                left: -50%;
                width: 200%;
                height: 200%;
                background: linear-gradient(120deg, rgba(255,255,255,0.2), transparent, rgba(255,255,255,0.2));
                transform: rotate(25deg);
                animation: shine 6s infinite linear;
            }}
            @keyframes shine {{
                0% {{ transform: translateX(-100%) rotate(25deg); }}
                100% {{ transform: translateX(100%) rotate(25deg); }}
            }}
            h1 {{
                margin-bottom: 15px;
                font-size: 34px;
                font-weight: 700;
                color: #ffeb3b;
                text-shadow: 0px 2px 10px rgba(0,0,0,0.5);
            }}
            .tagline {{
                font-size: 18px;
                margin-bottom: 30px;
                color: #f1f1f1;
                font-style: italic;
                text-shadow: 0px 1px 5px rgba(0,0,0,0.6);
            }}
            .value {{
                font-size: 26px;
                margin: 18px 0;
                font-weight: 600;
                color: #fff;
            }}
            .highlight {{
                color: #ff4b2b;
                font-weight: 700;
                font-size: 28px;
            }}
            a {{
                display: inline-block;
                margin-top: 30px;
                padding: 14px 28px;
                border-radius: 12px;
                text-decoration: none;
                background: linear-gradient(90deg, #ff416c, #ff4b2b);
                color: white;
                font-weight: 700;
                font-size: 18px;
                box-shadow: 0 5px 20px rgba(0,0,0,0.4);
                transition: 0.3s;
            }}
            a:hover {{
                transform: translateY(-3px) scale(1.05);
                box-shadow: 0 10px 30px rgba(0,0,0,0.6);
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Prediction Result</h1>
            <div class="tagline">Drive Smarter, Pay Smarter 🚗</div>
            <div class="value"><b>Predicted Price:</b> <span class="highlight">${pred:.2f}</span></div>
            <div class="value"><b>Difference from Ask Price:</b> <span class="highlight">${diff:.2f}</span></div>
            <a href="http://127.0.0.1:8000/">🔙 Back to Predictor</a>

        </div>
    </body>
    </html>
    """




# ================= VIEWS =================
from django.views.decorators.csrf import csrf_exempt

@csrf_exempt
def home(request):
    form = PredictionForm()
    return HttpResponse(landing_page(form))

@csrf_exempt
def predict(request):
    if request.method == "POST":
        form = PredictionForm(request.POST)
        if form.is_valid():
            mileage = form.cleaned_data["mileage"]
            ask_price = form.cleaned_data["ask_price"]

            with connection.cursor() as cur:
                cur.execute("""
                    SELECT mileage, predicted_price
                    FROM cars
                    ORDER BY ABS(mileage - %s)
                    LIMIT 1
                """, [mileage])
                row = cur.fetchone()

            if row:
                pred = row[1]
                diff = ask_price - pred
                return HttpResponse(result_page(pred, diff))
            else:
                return HttpResponse(result_page(0, 0))
    return HttpResponse("Invalid request")

# ================= URLS =================
from django.urls import path
from django.conf.urls.static import static

urlpatterns = [
    path("", home),
    path("predict", predict),
] + static(settings.STATIC_URL, document_root="static")

application = get_wsgi_application()

# ================= RUN SERVER =================
if __name__ == "__main__":
    from wsgiref.simple_server import make_server
    print("🚗 Starting server at http://127.0.0.1:8000")
    server = make_server("127.0.0.1", 8000, application)
    server.serve_forever()
