export interface Zone {
    _id: string;
    name: string;
    country: string;
    location: {
        type: 'Polygon';
        coordinates: number[][][];
    };
    geoPoint: {
        type: 'Point';
        coordinates: number[];
    };
    status: boolean;
}
export interface Driver {
    driverId: string;
    lat: number;
    lng: number;
    score: number;
    isFavorite?: boolean;
    isBusy: boolean;
    isNew: boolean;
    lastUpdate: number;
    approvedZones: string[];
}

export interface DriverWithDistance extends Driver {
    distance: number;
    priority: number;
}

export interface Job {
    id: string;
    customerId: string;
    pickupLat: number;
    pickupLng: number;
    fare: number;
    vehicleType?: string;
    timestamp: number;
    excludeDrivers?: string[];
    zoneId?: string;
}

export interface ProcessingMetrics {
    rpcRequests: number;
    jobsProcessed: number;
    driversMatched: number;
    offersSent: number;
    errors: number;
    apiCalls: {
        handlePaymentCompleted: number;
        handleDriverResponse: number;
        handleGetStats: number;
        handleHealthCheck: number;
        addJobs: number;
        addJob: number;
    };
}

export interface OfferData {
    jobId: string;
    driverId: string;
    customerId: string;
    pickupLat: number;
    pickupLng: number;
    fare: number;
    vehicleType?: string;
    status: 'pending' | 'accepted' | 'rejected' | 'expired';
    sentAt: string;
    expiresAt: number;
}

export interface RejectionData {
    jobId: string;
    driverId: string;
    reason: string;
    timestamp: string;
}

export interface JobStatus {
    pending: number;
    rejected: number;
    assigned: boolean;
}

export interface DriverTiers {
    within3km: DriverWithDistance[];
    within5km: DriverWithDistance[];
    within15km: DriverWithDistance[];
}

export interface MatchResult {
    driverId: string;
    priority: number;
    distance: number;
}