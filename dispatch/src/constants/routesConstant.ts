/**
 * routes/index.ts
 * -----------------------------------------------------
 * Central place to define all API route paths & versions.
 * Keeps our routing consistent and future-proof.
 */

export const API_VERSION = 'v1';
export const API_BASE = `/api/${API_VERSION}`;

export const ROUTES = {
    RIDE: {
        ROOT: `${API_BASE}/ride`,
        REQUEST: '/request',
        STATUS: '/status/:requestId',
        CANCEL: '/cancel/:requestId',
        HISTORY: '/history/:customerId',
    },
    OFFER: {
        ROOT: `${API_BASE}/offer`,
        ACCEPT: '/accept',
        REJECT: '/reject',
        JOB_OFFERS: '/job/:jobId',
        DISPATCH: '/dispatch',
    },
    MATCH: {
        ROOT: `${API_BASE}/match`,
        FIND: '/find',
        PRIORITY_BREAKDOWN: '/priority-breakdown',
        TEST: '/test/:type',
    },
    DRIVER: {
        ROOT: `${API_BASE}/driver`,
        LOCATION: '/location',
        PROFILE: '/profile',
        LOCATION_BY_ID: '/location/:driverId',
        FAVORITES: '/favorites',
    },
    HEALTH: {
        ROOT: `${API_BASE}/health`,
        REDIS: '/redis',
        KAFKA: '/kafka',
    },
};