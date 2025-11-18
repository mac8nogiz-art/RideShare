const { Kafka } = require('kafkajs');

// Kafka Configuration
const KAFKA_BROKER = '172.105.61.99:9093';
const TOPIC_NAME = 'newJob.request';

// Initialize Kafka client
const kafka = new Kafka({
    clientId: 'booking-test-producer',
    brokers: [KAFKA_BROKER],

});

// Create producer
const producer = kafka.producer();

// Event payload from your document
const eventPayload = {
    "type": "newBookingPlaced",
    "payload": {
        "_id": "6900561da9ea6f1301d29a",
        "orderNo": "17616297252927556",
        "serviceType": "rideBooking",
        "estimatedDirection": "",
        "actualDirection": "",
        "driverRouteString": "",
        "country": {
            "name": "canada",
            "countryCode": "1",
            "currencyCode": "CAD",
            "currencySymbol": "$"
        },
        "coupon": "68aee53843a601e539471117",
        "tripAddress": [
            {
                "markerType": "origin",
                "title": "122 Level 1st Floor D-199 Phase 8b Industrial Area Sector 74 Sahibzada Ajit Singh Nagar, Sector 74",
                "address": "122 Level 1st Floor D-199 Phase 8b Industrial Area Sector 74 Sahibzada Ajit Singh Nagar, Sector 74, 160055, Industrial Area, Sas Nagar, SAS Nagar, Punjab, India",
                "location": {
                    "latitude": 30.739374772617307,
                    "longitude": 76.68923699577694
                }
            },
            {
                "markerType": "origin",
                "title": "122 Level 1st Floor D-199 Phase 8b Industrial Area Sector 74 Sahibzada Ajit Singh Nagar, Sector 74",
                "address": "122 Level 1st Floor D-199 Phase 8b Industrial Area Sector 74 Sahibzada Ajit Singh Nagar, Sector 74, 160055, Industrial Area, Sas Nagar, SAS Nagar, Punjab, India",
                "location": {
                    "latitude": 30.705628515298216,
                    "longitude": 76.6849913165928
                }
            }
        ],
        "firstTripAddressGeoLocation": {
            "type": "Point",
            "coordinates": [76.6883, 30.7088]
        },
        "notesType": [],
        "lost": {
            "itemType": [],
            "seatType": "",
            "contact": ""
        },
        "selectedVehicle": {
            "name": "Swift Ride",
            "icon": "https://s3.ca-central-1.wasabisys.com/rapidoride/sedan.png",
            "seats": 4,
            "status": true,
            "surgeCharge": 0,
            "surgeValue": 1,
            "weatherSurge": {
                "underSurge": 0,
                "weather_code": ""
            },
            "vehiclePrice": 7.15,
            "subTotal": 8.22,
            "price": 9.19,
            "operatingFee": 0.07,
            "bookingFee": 1,
            "discount": 7.399100000000001,
            "forReservationPrice": {
                "price": 0,
                "tax": 0
            },
            "pricing": [
                { "name": "Fare", "price": 7.15 },
                { "name": "Booking Fee", "price": 1 },
                { "name": "Operating Fee", "price": 0.07 },
                { "name": "Reservation Fee", "price": 0 },
                { "name": "Surge Charge", "price": 0 },
                { "name": "Tax", "price": 0.97 },
                { "name": "Discount", "price": 1.7874999999999996 }
            ],
            "tax": {
                "percentage": 15,
                "taxTotal": 0.97
            },
            "discountObject": {
                "id": "68aee53843a601e539471117",
                "code": "1YEAR25",
                "discount": 25,
                "discountType": "percentage",
                "uptoAmount": 50,
                "isApplied": true
            },
            "km": 0.6,
            "kmText": "0.6 km",
            "durationText": "2 mins",
            "duration": 2,
            "carDuration": 0,
            "isAvailable": false,
            "pricingModalId": "69005611a9ea6f1305d29a9a"
        },
        "customer": {
            "_id": "68b928a6cefa75145c9d202e",
            "fullName": "Rahulee",
            "userID": "UID536750",
            "password": "",
            "phone": "7060810244",
            "email": "rahul@yopmail.com",
            "gender": "male",
            "rating": 5,
            "dob": null,
            "socket_id": "68b928a6cefa75145c9d202e",
            "inviteBy": null,
            "inviteCode": "BQDLFWWD",
            "avatar": "https://ui-avatars.com/api/?background=1E1E1E&color=BFC5EE&name=",
            "wallet": 11087.43,
            "isVerified": false,
            "otp": null,
            "otpExpireAt": null,
            "rideCount": 35,
            "fcmToken": "",
            "shareLocWithDriver": false,
            "theme": "light",
            "otpCheckCount": 0,
            "deletedAt": null,
            "country": {
                "name": "IN",
                "countryCode": "91",
                "currencySymbol": "$",
                "currencyCode": "CAD"
            },
            "paymentGatewayCustomerId": {
                "testStripeCustomerId": "",
                "stripeCustomerId": "",
                "testSquareCustomerId": "",
                "SquareCustomerId": ""
            },
            "socialAccount": {
                "google": null,
                "facebook": null,
                "apple": null,
                "instagram": null,
                "linkdin": null
            },
            "location": {
                "default": { "coordinates": [] },
                "type": "Point",
                "coordinates": []
            },
            "heading": 0,
            "favDrivers": ["68cbf8763cdfa2b2b69eeb5a"],
            "blockDrivers": [],
            "defaultOtpCode": "random",
            "defaultOtpCodeValue": "",
            "isBlocked": null,
            "createdAt": "2025-09-04T05:50:30.033Z",
            "updatedAt": "2025-10-28T05:34:14.334Z",
            "__v": 0,
            "extraFields": {
                "testStripeCustomerId": "cus_SzV4P1hEsKoI9k",
                "stripeCustomerId": null,
                "testSquareCustomerId": "8S0AQGK1DZ92N6HY51MF2X67P4",
                "squareCustomerId": null
            },
            "deviceInfo": {
                "apiLevel": -1,
                "androidId": "unknown",
                "baseOs": "unknown",
                "device": "unknown",
                "deviceName": "iPhone 16 Pro",
                "isTabletMode": false,
                "brand": "Apple",
                "deviceId": "iPhone17,1",
                "model": "iPhone 16 Pro",
                "systemName": "iOS",
                "systemVersion": "18.1",
                "isLowRamDevice": false,
                "isDisplayZoomed": false,
                "appVersion": "1.0.65",
                "deviceOS": "ios"
            }
        },
        "missedJobRequestDrivers": [],
        "rejectedDriver": [],
        "askDrivers": [],
        "askBusyDrivers": [],
        "askDriver": {
            "driver": null,
            "expTime": null
        },
        "otherVehicleInfo": [],
        "matchJobDrivers": [],
        "priorityDrivers": [],
        "tip": 0,
        "tipStatus": "Failed",
        "carPoolDetails": {
            "isBookingUnderPool": false,
            "bookingPoolDetails": null
        },
        "tax": {
            "percentage": 15,
            "taxTotal": 0.97
        },
        "grandTotal": 7.4,
        "tripStatus": "finding_driver",
        "canceledBy": "",
        "canceledReason": "",
        "paymentId": "",
        "paymentMethodId": "wallet",
        "paymentIntentId": "",
        "waitingChargesIntent": "",
        "otp": 3709,
        "expectedBilling": {
            "km": 0.6,
            "kmText": "0.6 km",
            "duration": 2,
            "durationText": "2 mins",
            "pricing": [
                { "name": "Fare", "price": 7.15 },
                { "name": "Booking Fee", "price": 1 },
                { "name": "Operating Fee", "price": 0.07 },
                { "name": "Reservation Fee", "price": 0 },
                { "name": "Surge Charge", "price": 0 },
                { "name": "Tax", "price": 0.97 },
                { "name": "Discount", "price": 1.7874999999999996 }
            ],
            "driverEarning": {
                "fare": 7.220000000000001,
                "waitingPrice": 0,
                "cancellationPrice": 0,
                "forReservationPrice": 0,
                "serviceFee": 1.8050000000000002,
                "otherEarning": 0,
                "tax": 0.27075,
                "driverTax": 1.083,
                "tips": 0,
                "subTotal": 5.144250000000001,
                "expenses": 0.15432750000000003,
                "grandTotal": 6.0729225000000016,
                "_id": "6900561da9ea6f1305d29abb"
            },
            "userBilling": {
                "routeFare": 8.22,
                "tax": {
                    "percentage": 15,
                    "taxTotal": 0.97
                },
                "tip": 0,
                "cancellationCharges": 0,
                "extraCharges": [],
                "discount": 1.79,
                "totalAmount": 7.4,
                "_id": "6900561da9ea6f1305d29abc"
            }
        },
        "finalBilling": {
            "km": 0.6,
            "kmText": "0.6 km",
            "duration": 2,
            "durationText": "2 mins",
            "pricing": [
                { "name": "Fare", "price": 7.15 },
                { "name": "Booking Fee", "price": 1 },
                { "name": "Operating Fee", "price": 0.07 },
                { "name": "Reservation Fee", "price": 0 },
                { "name": "Surge Charge", "price": 0 },
                { "name": "Tax", "price": 0.97 },
                { "name": "Discount", "price": 1.7874999999999996 }
            ],
            "driverEarning": {
                "fare": 7.220000000000001,
                "waitingPrice": 0,
                "cancellationPrice": 0,
                "forReservationPrice": 0,
                "serviceFee": 1.8050000000000002,
                "otherEarning": 0,
                "tax": 0.27075,
                "driverTax": 1.083,
                "tips": 0,
                "subTotal": 5.144250000000001,
                "expenses": 0.15432750000000003,
                "grandTotal": 6.0729225000000016,
                "_id": "6900561da9ea6f1305d29abd"
            },
            "userBilling": {
                "routeFare": 8.22,
                "tax": {
                    "percentage": 15,
                    "taxTotal": 0.97
                },
                "tip": 0,
                "cancellationCharges": 0,
                "extraCharges": [],
                "discount": 1.79,
                "totalAmount": 7.4,
                "_id": "6900561da9ea6f1305d29abe"
            }
        },
        "pickedAt": null,
        "dropedAt": null,
        "acceptedAt": null,
        "arrivedAt": null,
        "maxReachingSeconds": 0,
        "matchJobDistance": 0,
        "pickUpKm": 0,
        "pickUpTime": 0,
        "switchRider": {
            "bookRide": "SELF",
            "passenger": 1
        },
        "shareLocWithDriver": false,
        "time": "",
        "waitingTime": [],
        "isForce": false,
        "isThanks": false,
        "paymentStatus": true,
        "paymentStep": "succeeded",
        "nearByNotification": false,
        "noShowSmsCall": false,
        "scheduled": {
            "isScheduled": false,
            "scheduledAt": null,
            "OneHourBeforeNotification": false,
            "fourtyFiveBeforeNotification": false,
            "fifteenBeforeNotification": false,
            "fiveBeforeNotification": false,
            "startRide": false
        },
        "searchingCompleted": false,
        "searchingCount": 0,
        "autoReplacedJob": 0,
        "sharedRide": false,
        "lastMatchedAt": null,
        "priorityMatchedAt": null,
        "cancelledAt": null,
        "wayPointsTripStatus": [],
        "cancelledByDriver": [],
        "createdAt": "2025-10-28T05:35:25.307Z",
        "updatedAt": "2025-10-28T05:35:25.307Z",
        "__v": 0
    }
};

// Main function to send message
async function sendBookingEvent() {
    try {
        console.log('🔌 Connecting to Kafka broker:', KAFKA_BROKER);
        await producer.connect();
        console.log('✅ Connected to Kafka');

        console.log('📤 Sending message to topic:', TOPIC_NAME);

        const result = await producer.send({
            topic: TOPIC_NAME,
            messages: [
                {
                    key: eventPayload.payload._id,
                    value: JSON.stringify(eventPayload),
                    headers: {
                        'event-type': eventPayload.type,
                        'content-type': 'application/json',
                        'timestamp': new Date().toISOString()
                    }
                }
            ]
        });

        console.log('✅ Message sent successfully!');
        console.log('📊 Result:', JSON.stringify(result, null, 2));
        console.log('\n📋 Event Details:');
        console.log('   - Event Type:', eventPayload.type);
        console.log('   - Booking ID:', eventPayload.payload._id);
        console.log('   - Order No:', eventPayload.payload.orderNo);
        console.log('   - Customer:', eventPayload.payload.customer.fullName);
        console.log('   - Total Amount: CAD $' + eventPayload.payload.grandTotal);

    } catch (error) {
        console.error('❌ Error occurred:', error);
        throw error;
    } finally {
        await producer.disconnect();
        console.log('🔌 Disconnected from Kafka');
    }
}

// Execute the function
sendBookingEvent()
    .then(() => {
        console.log('\n✨ Script completed successfully!');
        process.exit(0);
    })
    .catch((error) => {
        console.error('\n💥 Script failed:', error);
        process.exit(1);
    });