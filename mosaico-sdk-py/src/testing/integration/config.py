import logging as log
from mosaicolabs.models.sensors import IMU, GPS, Image

TESTS_HOST = "127.0.0.1"
TESTS_PORT = 6276
log.basicConfig(level=log.DEBUG, format="%(levelname)s - %(message)s")


# ----- Sequence setup ----

UPLOADED_SEQUENCE_NAME = "test-sequence-datastream"
UPLOADED_SEQUENCE_METADATA = {
    "status": "processed",
    "visibility": "private",
    # --- Acquisition metadata ---
    "location": {
        "city": "Milan",
        "country": "IT",
        "facility": "Downtown",
        "gps": {
            "lat": 45.46481,
            "lon": 9.19201,
        },
    },
    "environment": {
        "weather": "sunny",
        "temperature_c": 17.6,
        "road_condition": "dry",
        "lighting": "daylight",
    },
    # --- Vehicle & driver ---
    "vehicle": {
        "vehicle_id": "veh_sim_042",
        "powertrain": "EV",
        "sensor_rig_version": "v3.2.1",
        "software_stack": {
            "perception": "perception-5.14.0",
            "localization": "loc-2.9.3",
            "planning": "plan-4.1.7",
        },
    },
    "driver": {
        "driver_id": "drv_sim_017",
        "role": "validation",
        "experience_level": "senior",
    },
    # --- Project & customer ---
    "project": {
        "project_id": "prj_9053",
        "name": "Urban Autonomy Validation",
        "version": "2.9.0",
        "milestone": "MVP-3",
        "customer": {
            "customer_id": "cust_3204",
            "type": "partner",
            "contract_tier": "gold",
        },
    },
    # --- Data volume & quality ---
    "data_volume": {
        "duration_s": 1842.4,
        "raw_size_gb": 312.8,
        "compressed_size_gb": 118.6,
        "frame_count": 55272,
    },
    "quality_metrics": {
        "dropped_frames_pct": 0.21,
        "gps_outages": 0,
        "imu_bias_detected": False,
        "sensor_desync_ms_max": 1.8,
        "overall_quality_score": 0.97,
    },
    # --- Access & auditing ---
    "access_control": {
        "owner_team": "perception-validation",
        "shared_with": ["planning-team", "qa-automation"],
        "public": False,
    },
    "audit": {
        "uploaded_by": "svc_uploader_01",
        "last_modified_by": "svc_postproc_07",
        "last_modified_at": "2025-03-01T14:49:55.112Z",
    },
}


# ----- Topics setup ----

UPLOADED_IMU_FRONT_TOPIC = "/front/imu"
UPLOADED_IMU_FRONT_FRAME_ID = "body"
UPLOADED_IMU_FRONT_METADATA = {
    "sensor_id": "imu_front_01",
    "role": "front",
    "vendor": "inertix-dynamics",
    "model": "ixd-f100",
    "firmware_version": "1.2.0",
    "serial_number": "IMUF-9A31D72X",
    "status": "active",
    "mounted_at": "2025-03-01T09:12:44Z",
    "last_calibrated_at": "2025-03-01T09:18:02Z",
    "calibration_version": "cal-2025.03.01",
    "update_rate_hz": 200,
    "noise_density": 0.0021,
    "bias_stability": 0.015,
    "interface": {
        "type": "CAN-FD",
        "baudrate": 2_000_000,
    },
}

UPLOADED_IMU_CAMERA_TOPIC = "/camera/left/imu"
UPLOADED_IMU_CAMERA_FRAME_ID = "roof-camera-left"
UPLOADED_IMU_CAMERA_METADATA = {
    "sensor_id": "imu_cam_01",
    "role": "camera_rig",
    "vendor": "gyrolytics",
    "model": "gl-c210",
    "firmware_version": "2.1.0",
    "serial_number": "IMUC-4F8B1C9Q",
    "status": "active",
    "mounted_at": "2025-03-01T09:13:10Z",
    "last_calibrated_at": "2025-03-01T09:19:07Z",
    "calibration_version": "cal-2025.03.01",
    "update_rate_hz": 400,
    "noise_density": 0.0014,
    "bias_stability": 0.010,
    "interface": {
        "type": "Ethernet",
        "protocol": "UDP",
        "ip": "192.168.10.42",
        "port": 7500,
    },
}

UPLOADED_GPS_TOPIC = "/gps"
UPLOADED_GPS_FRAME_ID = "roof-center"
UPLOADED_GPS_METADATA = {
    "sensor_id": "gps_01",
    "role": "primary_gps",
    "vendor": "satnavics",
    "model": "snx-g500",
    "firmware_version": "3.2.0",
    "serial_number": "GPS-7C1F4A9B",
    "status": "active",
    "mounted_at": "2025-03-01T09:10:15Z",
    "last_calibrated_at": "2025-03-01T09:15:30Z",
    "calibration_version": "cal-2025.03.01",
    "update_rate_hz": 10,
    "accuracy_m": 0.8,
    "rtk_enabled": True,
    "interface": {
        "type": "UART",
        "baudrate": 115200,
        "protocol": "NMEA",
    },
}

UPLOADED_MAGNETOMETER_TOPIC = "/magn"
UPLOADED_MAGNETOMETER_FRAME_ID = "body"
UPLOADED_MAGNETOMETER_METADATA = {
    "sensor_id": "mag_01",
    "role": "imu_mag",
    "vendor": "magnetronix",
    "model": "mx-200",
    "firmware_version": "0.1.2",
    "serial_number": "MAG-2F9D8B3C",
    "status": "active",
    "mounted_at": "2025-03-01T09:11:05Z",
    "last_calibrated_at": "2025-03-01T09:16:45Z",
    "calibration_version": "cal-2025.03.01",
    "update_rate_hz": 100,
    "range_gauss": 8.0,
    "bias_offset": [0.002, -0.001, 0.0005],
    "interface": {
        "type": "I2C",
        "address": "0x1E",
    },
}

# NOTE: Mockup for loading N sequences for testing base queries.
# DO NOT EDIT!!! Tests assume the data are immutable
QUERY_SEQUENCES_MOCKUP = {
    "test-query-sequence-1": {
        "topics": [
            {"name": "/topic11", "metadata": {}, "ontology_type": IMU},
            {"name": "/topic12", "metadata": {}, "ontology_type": GPS},
        ],
        "metadata": {
            "status": "processed",
            "visibility": "private",
        },
    },
    "test-query-sequence-2": {
        "topics": [
            {"name": "/topic21", "metadata": {}, "ontology_type": GPS},
            {"name": "/topic22", "metadata": {}, "ontology_type": IMU},
            {"name": "/topic23", "metadata": {}, "ontology_type": IMU},
        ],
        "metadata": {
            "status": "raw",
            "visibility": "private",
        },
    },
    "test-query-sequence-3": {
        "topics": [
            {"name": "/topic31", "metadata": {}, "ontology_type": Image},
            {"name": "/topic32", "metadata": {}, "ontology_type": Image},
            {"name": "/topic33", "metadata": {}, "ontology_type": GPS},
        ],
        "metadata": {
            "status": "labeled",
            "visibility": "public",
        },
    },
    "test-query-sequence-4": {
        "topics": [
            {"name": "/topic41", "metadata": {}, "ontology_type": IMU},
            {"name": "/topic42", "metadata": {}, "ontology_type": GPS},
            {"name": "/topic43", "metadata": {}, "ontology_type": Image},
            {"name": "/topic44", "metadata": {}, "ontology_type": GPS},
        ],
        "metadata": {
            "status": "post-processed",
            "visibility": "none",
        },
    },
}
