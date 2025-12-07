# alerts_service.py - CLEANED PRODUCTION VERSION
import os
from datetime import datetime
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AlertService:
    def __init__(self):
        # Load configuration
        self.demo_mode = os.getenv('DEMO_MODE', 'True').lower() == 'true'
        self.twilio_sid = os.getenv('TWILIO_ACCOUNT_SID', '').strip()
        self.twilio_token = os.getenv('TWILIO_AUTH_TOKEN', '').strip()
        self.twilio_phone = os.getenv('TWILIO_PHONE_NUMBER', '').strip()
        self.admin_phone = os.getenv('ADMIN_PHONE', '').strip()
        
        # WhatsApp configuration
        self.whatsapp_enabled = os.getenv('WHATSAPP_ENABLED', 'False').lower() == 'true'
        self.whatsapp_admin = os.getenv('WHATSAPP_ADMIN_NUMBER', '').strip()
        self.whatsapp_sandbox = os.getenv('WHATSAPP_SANDBOX_NUMBER', '+14155238886').strip()
        
        # Load threshold
        try:
            threshold_str = os.getenv('ALERT_THRESHOLD', '0.85')
            self.alert_threshold = float(threshold_str)
            if not 0.0 <= self.alert_threshold <= 1.0:
                self.alert_threshold = 0.85
        except (ValueError, TypeError):
            self.alert_threshold = 0.85
        
        # Initialize Twilio client
        self.twilio_client = None
        if not self.demo_mode and self.twilio_sid and self.twilio_token:
            try:
                from twilio.rest import Client
                
                if not self.twilio_sid.startswith('AC'):
                    self.demo_mode = True
                elif len(self.twilio_sid) < 30:
                    self.demo_mode = True
                else:
                    self.twilio_client = Client(self.twilio_sid, self.twilio_token)
                        
            except ImportError:
                logger.error("Twilio package not installed")
                self.demo_mode = True
            except Exception as e:
                logger.error(f"Twilio initialization failed: {e}")
                self.demo_mode = True
        else:
            if self.demo_mode:
                logger.info("Running in DEMO mode")
            else:
                logger.warning("Missing Twilio credentials, using demo mode")
                self.demo_mode = True
        
        # Check WhatsApp setup
        if self.whatsapp_enabled and not self.whatsapp_admin:
            logger.warning("WhatsApp enabled but no admin number configured")
            self.whatsapp_enabled = False
        
        logger.info(f"Alert Service initialized - Mode: {'DEMO' if self.demo_mode else 'PRODUCTION'}")
    
    def create_sms_message(self, transaction_data, prediction_result):
        """Create SMS message for fraud alert"""
        fraud_score = prediction_result.get('fraud_score', 0)
        
        # Truncate long transaction IDs
        txn_id = transaction_data.get('transaction_id', 'UNKNOWN')
        if len(txn_id) > 12:
            txn_id = txn_id[-12:]
        
        # Get location
        location = transaction_data.get('city', 'Unknown')
        
        message = f"""🚨 FRAUD ALERT 🚨

TX: {txn_id}
Card: ****{transaction_data.get('cc_number', '****')[-4:]}
Amount: ₹{transaction_data.get('amount', 0):,.2f}
Location: {location}

Confidence: {fraud_score:.1%}
Reason: {prediction_result.get('reason', 'Suspicious activity')}

Time: {datetime.now().strftime('%H:%M:%S')}

⚠️ Immediate verification required."""
        
        # Ensure message is within SMS limits
        if len(message) > 1500:
            message = message[:1497] + "..."
        
        return message
    
    def create_whatsapp_message(self, transaction_data, prediction_result):
        """Create WhatsApp message with formatting"""
        fraud_score = prediction_result.get('fraud_score', 0)
        
        # Truncate long transaction IDs
        txn_id = transaction_data.get('transaction_id', 'UNKNOWN')
        if len(txn_id) > 12:
            txn_id = txn_id[-12:]
        
        message = f"""*🚨 URGENT: FRAUD DETECTED* 🚨

*Transaction Details:*
• *ID:* {txn_id}
• *Card:* ****{transaction_data.get('cc_number', '****')[-4:]}
• *User:* {transaction_data.get('user_id', 'N/A')}
• *Amount:* ₹{transaction_data.get('amount', 0):,.2f}
• *Location:* {transaction_data.get('city', 'Unknown')}
• *Time:* {datetime.now().strftime('%H:%M:%S')}

*Fraud Analysis:*
• *Confidence:* {fraud_score:.1%}
• *Risk Level:* {'🔥 CRITICAL' if fraud_score > 0.9 else '⚠️ HIGH'}
• *Reason:* {prediction_result.get('reason', 'Suspicious activity')}

*🛡️ Action Required:* 
Please review immediately and block card if suspicious.

_Automated alert from Fraud Detection System_"""
        
        return message
    
    def send_real_sms(self, message):
        """Send real SMS via Twilio"""
        try:
            if not self.twilio_client:
                return {
                    "success": False,
                    "error": "Twilio client not available",
                    "type": "SMS"
                }
            
            if not self.admin_phone or len(self.admin_phone) < 10:
                return {
                    "success": False,
                    "error": "Invalid phone number",
                    "type": "SMS"
                }
            
            sms = self.twilio_client.messages.create(
                body=message,
                from_=self.twilio_phone,
                to=self.admin_phone
            )
            
            logger.info(f"SMS sent to {self.admin_phone}")
            
            return {
                "success": True,
                "sid": sms.sid,
                "status": sms.status,
                "type": "SMS",
                "to": self.admin_phone,
                "mode": "production"
            }
            
        except Exception as e:
            logger.error(f"SMS failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "type": "SMS",
                "mode": "error"
            }
    
    def send_real_whatsapp(self, message):
        """Send real WhatsApp via Twilio"""
        try:
            if not self.whatsapp_enabled:
                return {
                    "success": False,
                    "error": "WhatsApp disabled",
                    "type": "WhatsApp"
                }
            
            if not self.twilio_client:
                return {
                    "success": False,
                    "error": "Twilio client not available",
                    "type": "WhatsApp"
                }
            
            if not self.whatsapp_admin:
                return {
                    "success": False,
                    "error": "No WhatsApp number",
                    "type": "WhatsApp"
                }
            
            # Format numbers for WhatsApp
            from_whatsapp = f"whatsapp:{self.whatsapp_sandbox}"
            to_whatsapp = f"whatsapp:{self.whatsapp_admin}"
            
            whatsapp_msg = self.twilio_client.messages.create(
                body=message,
                from_=from_whatsapp,
                to=to_whatsapp
            )
            
            logger.info(f"WhatsApp sent to {self.whatsapp_admin}")
            
            return {
                "success": True,
                "sid": whatsapp_msg.sid,
                "status": whatsapp_msg.status,
                "type": "WhatsApp",
                "to": self.whatsapp_admin,
                "mode": "production"
            }
            
        except Exception as e:
            logger.error(f"WhatsApp failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "type": "WhatsApp",
                "mode": "error"
            }
    
    def send_demo_sms(self, message):
        """Simulate SMS sending (demo mode)"""
        logger.info(f"DEMO: SMS would be sent to {self.admin_phone}")
        
        return {
            "success": True,
            "type": "SMS",
            "to": self.admin_phone,
            "mode": "demo"
        }
    
    def send_demo_whatsapp(self, message):
        """Simulate WhatsApp sending (demo mode)"""
        logger.info(f"DEMO: WhatsApp would be sent to {self.whatsapp_admin}")
        
        return {
            "success": True,
            "type": "WhatsApp",
            "to": self.whatsapp_admin,
            "mode": "demo"
        }
    
    def trigger_alerts(self, transaction_data, prediction_result):
        """Main method to trigger SMS & WhatsApp alerts"""
        fraud_score = prediction_result.get('fraud_score', 0)
        transaction_id = transaction_data.get('transaction_id', 'UNKNOWN')
        
        # Check threshold
        if fraud_score < self.alert_threshold:
            logger.info(f"Fraud score {fraud_score:.1%} below threshold {self.alert_threshold:.1%}")
            return {
                "triggered": False,
                "reason": f"Below threshold ({fraud_score:.1%} < {self.alert_threshold:.1%})"
            }
        
        logger.info(f"High fraud detected: {fraud_score:.1%} - Sending alerts...")
        
        # Create messages
        sms_message = self.create_sms_message(transaction_data, prediction_result)
        whatsapp_message = self.create_whatsapp_message(transaction_data, prediction_result)
        
        # Initialize alert results
        alert_results = {
            "triggered": True,
            "fraud_score": fraud_score,
            "threshold": self.alert_threshold,
            "transaction_id": transaction_id,
            "alerts": {},
            "mode": "demo" if self.demo_mode else "production"
        }
        
        # Send SMS
        if self.demo_mode:
            alert_results["alerts"]["sms"] = self.send_demo_sms(sms_message)
        else:
            alert_results["alerts"]["sms"] = self.send_real_sms(sms_message)
        
        # Send WhatsApp (if enabled)
        if self.whatsapp_enabled:
            if self.demo_mode:
                alert_results["alerts"]["whatsapp"] = self.send_demo_whatsapp(whatsapp_message)
            else:
                alert_results["alerts"]["whatsapp"] = self.send_real_whatsapp(whatsapp_message)
        else:
            alert_results["alerts"]["whatsapp"] = {
                "success": False,
                "error": "WhatsApp disabled in settings",
                "type": "WhatsApp"
            }
        
        # Log transaction
        logger.info(f"Transaction: {transaction_id}, Amount: ₹{transaction_data.get('amount', 0):,.2f}")
        
        return alert_results

# Create singleton instance
alert_service = AlertService()