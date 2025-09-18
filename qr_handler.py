import os
import re
import qrcode
from datetime import datetime, timedelta
from flask import request, current_app

class QRHandler:
    def __init__(self, app):
        self.app = app
        self.qr_folder = app.config.get('QR_FOLDER', 'static/qrcodes')
        os.makedirs(self.qr_folder, exist_ok=True)

    def sanitize_filename(self, name):
        """Convert hall name to safe filename"""
        name = re.sub(r'[^\w\s-]', '', name).strip().lower()
        return re.sub(r'[-\s]+', '_', name)

    def generate_qr_code(self, program_id, training_name, location_hall, start_datetime, end_datetime, duration_days):
        """Generate both attendance and feedback QR codes for a program"""
        try:
            # Attendance QR
            attendance_filename = self._generate_single_qr(
                program_id=program_id,
                qr_type='attendance',
                url_path=f"/attendance/{program_id}",
                fill_color="#160272",  # Dark blue
                back_color="#f0f0f0"
            )

            # Feedback QR
            feedback_filename = self._generate_single_qr(
                program_id=program_id,
                qr_type='feedback',
                url_path=f"/feedback/form/{program_id}",
                fill_color="#015B01BC",  # Dark green
                back_color="#ffffff"
            )

            current_app.logger.info(f"Generated both QR codes for program {program_id}")
            return {
                'attendance': attendance_filename,
                'feedback': feedback_filename
            }

        except Exception as e:
            current_app.logger.error(f"Error generating QR codes: {e}")
            raise

    def _generate_single_qr(self, program_id, qr_type, url_path, fill_color, back_color):
        """Helper to generate a single QR code"""
        qr_url = request.host_url.rstrip('/') + url_path
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_H,
            box_size=8,
            border=2,
        )
        qr.add_data(qr_url)
        qr.make(fit=True)

        img = qr.make_image(
            fill_color=fill_color,
            back_color=back_color
        )

        filename = f"{qr_type}_program_{program_id}.png"
        filepath = os.path.join(self.qr_folder, filename)
        img.save(filepath, quality=90)

        return filename

    def get_qr_path(self, program_id, qr_type='attendance'):
        """Get path to QR code file for a program"""
        if qr_type not in ['attendance', 'feedback']:
            raise ValueError("Invalid QR type. Must be 'attendance' or 'feedback'")

        filename = f"{qr_type}_program_{program_id}.png"
        filepath = os.path.join(self.qr_folder, filename)

        if not os.path.exists(filepath):
            current_app.logger.warning(f"{qr_type.capitalize()} QR code not found for program {program_id}")
            return None

        return filepath

    def generate_hall_qr_code(self, hall_name):
        """Generate a generic QR code for a hall with enhanced security"""
        try:
            sanitized_hall = self.sanitize_filename(hall_name)
            hall_url = request.host_url.rstrip('/') + f"/attendance/hall/{sanitized_hall}"

            data = {
                'version': 2,
                'hall': sanitized_hall,
                'timestamp': datetime.now().isoformat(),
                'url': hall_url,
                'checksum': self._generate_checksum(hall_name)
            }

            qr = qrcode.QRCode(
                version=2,
                error_correction=qrcode.constants.ERROR_CORRECT_Q,
                box_size=6,
                border=4,
            )
            qr.add_data(data['url'])
            qr.make(fit=True)

            img = qr.make_image(fill_color="#006400", back_color="#ffffff")

            filename = f"hall_{sanitized_hall}.png"
            filepath = os.path.join(self.qr_folder, filename)
            img.save(filepath)

            return filename

        except Exception as e:
            current_app.logger.error(f"Error generating hall QR code: {e}")
            raise

    def get_hall_qr_filename(self, hall_name):
        """Return existing QR filename if exists, else generate new"""
        filename = f"hall_{self.sanitize_filename(hall_name)}.png"
        filepath = os.path.join(self.qr_folder, filename)

        if not os.path.exists(filepath):
            try:
                return self.generate_hall_qr_code(hall_name)
            except Exception as e:
                current_app.logger.error(f"Failed to generate hall QR: {e}")
                return None

        return filename

    def _generate_checksum(self, text):
        """Simple checksum for data validation"""
        return sum(ord(char) for char in text) % 10000

    def validate_qr_data(self, data):
        """Validate QR code data structure and time validity"""
        try:
            required_fields = [
                'program_id', 'training_name', 'location',
                'start_date', 'duration_days',
                'daily_start_time', 'daily_end_time',
                'qr_valid_from', 'qr_valid_to'
            ]

            if not all(field in data for field in required_fields):
                return False, "Invalid QR data structure"

            now = datetime.now()
            valid_from = datetime.fromisoformat(data['qr_valid_from'])
            valid_to = datetime.fromisoformat(data['qr_valid_to'])

            if now < valid_from:
                return False, f"QR not valid until {valid_from.strftime('%d/%m/%Y %H:%M')}"

            if now > valid_to:
                return False, f"QR expired on {valid_to.strftime('%d/%m/%Y %H:%M')}"

            start_date = datetime.fromisoformat(data['start_date']).date()
            current_day = (datetime.now().date() - start_date).days + 1

            if current_day < 1 or current_day > int(data['duration_days']):
                return False, "No active training session today"

            start_time = datetime.strptime(data['daily_start_time'], "%H:%M").time()
            end_time = datetime.strptime(data['daily_end_time'], "%H:%M").time()
            now_time = datetime.now().time()

            adjusted_start = (datetime.combine(datetime.today(), start_time) -
                              timedelta(minutes=15)).time()

            if not (adjusted_start <= now_time <= end_time):
                return False, (f"Attendance only valid between {adjusted_start.strftime('%H:%M')} "
                               f"and {end_time.strftime('%H:%M')}")

            return True, "Valid QR code"

        except Exception as e:
            current_app.logger.error(f"QR validation error: {e}")
            return False, "Invalid QR code data"
