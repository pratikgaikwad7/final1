from flask import Blueprint, render_template, request, url_for
from datetime import datetime

# Create Blueprint
user_bp = Blueprint('user', __name__, url_prefix='/user')

@user_bp.route('/dashboard')
def user_dashboard():
    """User dashboard (simple, no DB)"""
    
    # just pick which tab is active
    active_tab = request.args.get('tab', 'upcoming')
    
    return render_template(
        "user/dashboard.html",
        upcoming_programs=[],   # empty list instead of DB data
        tni_reports=[],         # empty list instead of DB data
        active_tab=active_tab,
        current_date=datetime.now().date()
    )

# -------------------
# Routes for each menu item
# -------------------

@user_bp.route('/induction')
def induction():
    return render_template("user/induction.html")

@user_bp.route('/fst')
def fst():
    return render_template("user/fst.html")

@user_bp.route('/fta')
def fta():
    return render_template("user/fta.html")

@user_bp.route('/jta_ta')
def jta_ta():
    return render_template("user/jta_ta.html")

@user_bp.route('/ta')
def ta():
    return render_template("user/ta.html")

@user_bp.route('/kaushalya')
def kaushalya():
    return render_template("user/kaushalya.html")

@user_bp.route('/pragati')
def pragati():
    return render_template("user/pragati.html")

@user_bp.route('/lakshya')
def lakshya():
    return render_template("user/lakshya.html")

@user_bp.route('/live_trainer')
def live_trainer():
    return render_template("user/live_trainer.html")