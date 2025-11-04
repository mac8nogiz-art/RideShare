import { t } from "elysia";
import { Value } from "@sinclair/typebox/value";

export const locationSchema = t.Object({
    latitude: t.Number({
        minimum: -90,
        maximum: 90,
        error: "Latitude must be between -90 and 90",
        description: "Latitude coordinate (-90 to 90)"
    }),
    longitude: t.Number({
        minimum: -180,
        maximum: 180,
        error: "Longitude must be between -180 and 180",
        description: "Longitude coordinate (-180 to 180)"
    })
});

export const tripAddressSchema = t.Object({
    markerType: t.String({ error: "Marker type is required" }),
    title: t.String({ error: "Title is required" }),
    address: t.String({ error: "Address is required" }),
    location: locationSchema
});

export const customerSchema = t.Object({
    _id: t.String({ error: "Customer ID is required" }),
    fullName: t.Optional(t.String()),
    avatar: t.Optional(t.String())
});


export const selectedVehicleSchema = t.Object({
    name: t.String({ error: "Vehicle name is required" })
});

export const jobPayloadSchema = t.Object({
    _id: t.String({ error: "Job ID is required" }),
    customer: customerSchema,
    tripAddress: t.Array(tripAddressSchema, {
        minItems: 1,
        error: "At least one trip address is required"
    }),
    grandTotal: t.Number({
        minimum: 0,
        error: "Grand total must be non-negative"
    }),
    selectedVehicle: selectedVehicleSchema,
    orderNo: t.Optional(t.String()),
    createdAt: t.Optional(t.String())
});


export const newJobEventSchema = t.Object({
    type: t.String({ error: "Event type is required" }),
    payload: jobPayloadSchema,
    bookingId: t.Optional(t.Nullable(t.String()))
});


export const driverResponseSchema = t.Object({
    driverId: t.String({ error: "Driver ID is required" }),
    jobId: t.String({ error: "Job ID is required" }),
    action: t.Union([
        t.Literal("accept"),
        t.Literal("reject"),
        t.Literal("statusUpdate"),
        t.Literal("canceled"),

    ], { error: "Action must be either accept or reject" }),
    reason: t.Optional(t.String())
});


export interface ValidationResult {
    valid: boolean;
    data?: any;
    errors?: string[];
}


export function validateSchema(schema: any, data: any): ValidationResult {

    if (data === null || data === undefined) {
        return {
            valid: false,
            errors: ["Data is null or undefined"]
        };
    }


    if (typeof data !== 'object') {
        return {
            valid: false,
            errors: [`Data must be an object, got ${typeof data}`]
        };
    }

    try {

        const valid = Value.Check(schema, data);

        if (valid) {
            return {
                valid: true,
                data
            };
        }


        const errors = [...Value.Errors(schema, data)].map((error: any) => {
            const path = error.path || '/';
            return `${path}: ${error.message}`;
        });

        return {
            valid: false,
            errors: errors.length ? errors : ["Unknown validation error"]
        };
    } catch (error: any) {
        return {
            valid: false,
            errors: [`Validation exception: ${error.message}`]
        };
    }
}

