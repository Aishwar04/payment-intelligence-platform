import uuid
import random
import numpy as np
from datetime import datetime, timezone, timedelta
from faker import Faker

from ingestion.simulator.config import (
    USER_CONFIG,
    MERCHANT_CONFIG,
    GATEWAY_CONFIG,
    PROCESSOR_CONFIG,
    PAYMENT_METHOD_CONFIG,
    FAILURE_REASONS,
    LOCATION_CONFIG,
    HOURLY_WEIGHTS,
    SIMULATION_CONFIG
)

# ─────────────────────────────────────────
# INITIALISE FAKER
# ─────────────────────────────────────────
fake = Faker("en_IN")


class TransactionGenerator:
    """
    Generates realistic payment transactions
    simulating a production payment platform.
    """

    def __init__(self):
        self.users = self._generate_user_pool()
        self.merchants = self._generate_merchant_pool()

    # ─────────────────────────────────────────
    # USER POOL

    # Creates 10,000 users. Each user is assigned a segment — premium, regular, new, or dormant — using weighted random choice.
    # 10% will be premium, 60% regular etc. Each user gets a fixed average transaction amount based on their segment.
    # ─────────────────────────────────────────
    def _generate_user_pool(self) -> list:
        """
        Pre-generates a pool of users at startup.
        This simulates a real user base where the
        same users transact repeatedly over time.
        """
        users = []
        segments = USER_CONFIG["segments"]
        segment_names = list(segments.keys())
        segment_weights = [segments[s]["weight"] for s in segment_names]

        for i in range(USER_CONFIG["total_users"]):
            segment = random.choices(segment_names, weights=segment_weights, k=1)[0]
            users.append({
                "user_id": f"USR-{str(i+1).zfill(6)}",
                "segment": segment,
                "avg_amount": segments[segment]["avg_amount"],
                "txn_frequency": segments[segment]["txn_frequency"]
            })
        return users

    # ─────────────────────────────────────────
    # MERCHANT POOL

    #   Creates 1,000 merchants. Crucially applies Zipf distribution for merchant weights. 
    #   Merchant 1 gets the most traffic, merchant 1000 gets almost none. This is how real payment data looks — a handful of 
    #   merchants like Swiggy, Amazon, Netflix dominate transaction volume.
    # ─────────────────────────────────────────
    def _generate_merchant_pool(self) -> list:
        """
        Pre-generates a pool of merchants using
        Zipf distribution — a few merchants get
        most transactions, most get very few.
        This mirrors real payment data where
        Swiggy/Amazon dominate transaction volume.
        """
        merchants = []
        categories = MERCHANT_CONFIG["categories"]
        category_names = list(categories.keys())
        category_weights = [categories[c]["weight"] for c in category_names]

        for i in range(MERCHANT_CONFIG["total_merchants"]):
            category = random.choices(category_names, weights=category_weights, k=1)[0]
            merchants.append({
                "merchant_id": f"MRC-{str(i+1).zfill(6)}",
                "category": category,
                "avg_amount": categories[category]["avg_amount"],
                "failure_rate": categories[category]["failure_rate"]
            })

        # apply zipf weights — merchant 1 gets most traffic
        # merchant 1000 gets very little
        zipf_weights = [1.0 / (i + 1) for i in range(len(merchants))]
        total = sum(zipf_weights)
        self.merchant_weights = [w / total for w in zipf_weights]

        return merchants

    # ─────────────────────────────────────────
    # PICK USER


    # Picks a user weighted by txn_frequency. Premium users transact 40% of the time, dormant users only 5%. 
    # So premium users appear far more often in the transaction stream.
    # ─────────────────────────────────────────
    def _pick_user(self) -> dict:
        """
        Picks a user weighted by transaction frequency.
        Premium and regular users transact more often
        than new or dormant users.
        """
        frequencies = [u["txn_frequency"] for u in self.users]
        return random.choices(self.users, weights=frequencies, k=1)[0]

    # ─────────────────────────────────────────
    # PICK MERCHANT
    # ─────────────────────────────────────────
    def _pick_merchant(self) -> dict:
        """
        Picks a merchant using Zipf distribution.
        Top merchants (Swiggy, Amazon equivalent)
        get disproportionately more transactions.
        """
        return random.choices(
            self.merchants,
            weights=self.merchant_weights,
            k=1
        )[0]

    # ─────────────────────────────────────────
    # PICK GATEWAY

# Only picks gateways that support the chosen payment method. 
# Razorpay supports UPI but not all gateways support all methods — this mirrors reality.

    # ─────────────────────────────────────────
    def _pick_gateway(self, payment_method: str) -> dict:
        """
        Picks a gateway that supports the payment method.
        Not all gateways support all payment methods.
        """
        eligible = {
            gw_id: gw
            for gw_id, gw in GATEWAY_CONFIG.items()
            if payment_method in gw["supported_methods"]
        }
        gateway_id = random.choice(list(eligible.keys()))
        return {"gateway_id": gateway_id, **eligible[gateway_id]}

    # ─────────────────────────────────────────
    # PICK PAYMENT METHOD
    # ─────────────────────────────────────────
    def _pick_payment_method(self) -> dict:
        """
        Picks a payment method weighted by usage.
        UPI dominates at 55% — realistic for India.
        """
        methods = list(PAYMENT_METHOD_CONFIG.keys())
        weights = [PAYMENT_METHOD_CONFIG[m]["weight"] for m in methods]
        method = random.choices(methods, weights=weights, k=1)[0]
        return {"method": method, **PAYMENT_METHOD_CONFIG[method]}

    # ─────────────────────────────────────────
    # PICK PROCESSOR
    # ─────────────────────────────────────────
    def _pick_processor(self, payment_method: str) -> dict:
        """
        Maps payment method to its processor.
        UPI always goes through NPCI.
        Cards go through Visa or Mastercard randomly.
        """
        method_config = PAYMENT_METHOD_CONFIG[payment_method]
        processor_id = method_config["processor"]

        # cards can go through either Visa or Mastercard
        if payment_method == "CARD":
            processor_id = random.choice(["PRC-002", "PRC-003"])

        return {
            "processor_id": processor_id,
            **PROCESSOR_CONFIG[processor_id]
        }

    # ─────────────────────────────────────────
    # DETERMINE TRANSACTION STATUS
    # This is the most important method for our ML model. 
    # Failure probability is a weighted combination of three factors — 
    # merchant category risk, payment method risk, and gateway reliability. 
    # This creates realistic, learnable patterns that our LSTM model will be trained to predict.

    # ─────────────────────────────────────────
    def _determine_status(
        self,
        merchant: dict,
        payment_method_config: dict,
        gateway: dict
    ) -> tuple:
        """
        Determines if a transaction succeeds or fails.
        Failure probability is a combination of:
        - merchant category failure rate
        - payment method failure rate
        - gateway success rate
        Returns (status, failure_reason)
        """
        combined_failure_rate = (
            merchant["failure_rate"] * 0.4 +
            payment_method_config["failure_rate"] * 0.4 +
            (1 - gateway["success_rate"]) * 0.2
        )

        if random.random() < combined_failure_rate:
            reasons = list(FAILURE_REASONS.keys())
            weights = list(FAILURE_REASONS.values())
            reason = random.choices(reasons, weights=weights, k=1)[0]
            return "FAILED", reason

        return "SUCCESS", None

    # ─────────────────────────────────────────
    # GENERATE TRANSACTION AMOUNT
    # Uses normal distribution around the average of user segment and
    #  merchant category amounts. This creates realistic bell-curve distribution
    #  of amounts rather than uniform random. Minimum is Rs 10 — no negative or zero amounts.
    # ─────────────────────────────────────────
    def _generate_amount(self, user: dict, merchant: dict) -> float:
        """
        Generates a realistic transaction amount.
        Uses normal distribution around the average
        of user segment and merchant category amounts.
        Ensures amount is always positive and rounded
        to 2 decimal places.
        """
        avg = (user["avg_amount"] + merchant["avg_amount"]) / 2
        std = avg * 0.3  # 30% standard deviation for natural variance
        amount = np.random.normal(avg, std)
        return round(max(10.0, amount), 2)  # minimum transaction is Rs 10

    # ─────────────────────────────────────────
    # GENERATE TIMESTAMPS
    # Most important for our pipeline design. Generates two timestamps — txn_timestamp (when it happened) 
    # and arrival_timestamp (when it arrived). For 5% of transactions it backdates txn_timestamp by 1-7 days — 
    # these are our late arrivals. This is exactly what we designed our Iceberg MERGE and updated_at pattern to handle.
    # ─────────────────────────────────────────
    def _generate_timestamps(self) -> tuple:
        """
        Generates txn_timestamp and arrival_timestamp.

        txn_timestamp  = when the transaction actually happened
        arrival_timestamp = when the event arrived in our system

        For late arrivals (5% of transactions):
        txn_timestamp is backdated by 1-7 days.
        This simulates real scenarios where payment
        systems send delayed transaction records.

        This is exactly the late arrival problem we
        designed our Iceberg pipeline to handle.
        """
        now = datetime.now(timezone.utc)

        # simulate time of day patterns
        hour_weights = HOURLY_WEIGHTS
        hour = random.choices(range(24), weights=hour_weights, k=1)[0]
        minute = random.randint(0, 59)
        second = random.randint(0, 59)

        txn_time = now.replace(hour=hour, minute=minute, second=second)

        # late arrival simulation
        is_late = random.random() < SIMULATION_CONFIG["late_arrival_probability"]
        if is_late:
            days_late = random.randint(1, SIMULATION_CONFIG["late_arrival_max_days"])
            txn_time = txn_time - timedelta(days=days_late)

        arrival_time = now  # arrival is always now

        return (
            txn_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            arrival_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        )

    # ─────────────────────────────────────────
    # PICK LOCATION
    # ─────────────────────────────────────────
    def _pick_location(self) -> dict:
        """
        Picks a city weighted by transaction volume.
        Mumbai and Delhi get most transactions.
        """
        weights = [loc["weight"] for loc in LOCATION_CONFIG]
        location = random.choices(LOCATION_CONFIG, weights=weights, k=1)[0]
        return location

    # ─────────────────────────────────────────
    # GENERATE ONE TRANSACTION

    # The main method that assembles everything into one transaction dictionary. This is what gets published to Kafka.
    # ─────────────────────────────────────────
    def generate(self) -> dict:
        """
        Main method. Generates one complete
        realistic payment transaction event.
        This is what gets published to Kafka.
        """
        # pick all components
        user = self._pick_user()
        merchant = self._pick_merchant()
        payment_method_data = self._pick_payment_method()
        gateway = self._pick_gateway(payment_method_data["method"])
        processor = self._pick_processor(payment_method_data["method"])
        location = self._pick_location()
        amount = self._generate_amount(user, merchant)
        txn_timestamp, arrival_timestamp = self._generate_timestamps()
        status, failure_reason = self._determine_status(
            merchant, payment_method_data, gateway
        )

        return {
            # transaction identity
            "txn_id": f"TXN-{uuid.uuid4().hex[:12].upper()}",
            "txn_timestamp": txn_timestamp,
            "arrival_timestamp": arrival_timestamp,

            # transaction details
            "amount": amount,
            "currency": "INR",
            "status": status,
            "failure_reason": failure_reason,

            # user details
            "user_id": user["user_id"],
            "user_segment": user["segment"],

            # merchant details
            "merchant_id": merchant["merchant_id"],
            "merchant_category": merchant["category"],

            # payment routing
            "gateway_id": gateway["gateway_id"],
            "gateway_name": gateway["name"],
            "processor_id": processor["processor_id"],
            "processor_name": processor["name"],
            "payment_method": payment_method_data["method"],

            # device and location
            "device_type": random.choice(["mobile", "desktop", "tablet"]),
            "location_city": location["city"],
            "location_state": location["state"],
            "is_international": False
        }