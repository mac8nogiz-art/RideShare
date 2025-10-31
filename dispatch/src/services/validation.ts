import { t } from "elysia";

// ============= LOCATION SCHEMA =============
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

// ============= TRIP ADDRESS SCHEMA =============
export const tripAddressSchema = t.Object({
    markerType: t.String({ error: "Marker type is required" }),
    title: t.String({ error: "Title is required" }),
    address: t.String({ error: "Address is required" }),
    location: locationSchema
});

// ============= CUSTOMER SCHEMA =============
export const customerSchema = t.Object({
    _id: t.String({ error: "Customer ID is required" }),
    fullName: t.Optional(t.String()),
    avatar: t.Optional(t.String())
});

// ============= VEHICLE SCHEMA =============
export const selectedVehicleSchema = t.Object({
    name: t.String({ error: "Vehicle name is required" })
});

// ============= JOB PAYLOAD SCHEMA =============
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

// ============= NEW JOB EVENT SCHEMA =============
export const newJobEventSchema = t.Object({
    type: t.String({ error: "Event type is required" }),
    payload: jobPayloadSchema,
    bookingId: t.Optional(t.Nullable(t.String()))
});

// ============= DRIVER RESPONSE SCHEMA =============
export const driverResponseSchema = t.Object({
    driverId: t.String({ error: "Driver ID is required" }),
    jobId: t.String({ error: "Job ID is required" }),
    action: t.Union([
        t.Literal("accept"),
        t.Literal("reject")
    ], { error: "Action must be either accept or reject" }),
    reason: t.Optional(t.String())
});

// ============= VALIDATION RESULT INTERFACE =============
export interface ValidationResult {
    valid: boolean;
    data?: any;
    errors?: string[];
}

// ============= VALIDATION HELPER FUNCTION =============
export function validateSchema(schema: any, data: any): ValidationResult {
    const result = schema.safeParse(data);

    if (result.success) {
        return {
            valid: true,
            data: result.data
        };
    }

    const errors = result.error?.issues?.map((i: any) => i.message) || ["Unknown validation error"];
    return {
        valid: false,
        errors
    };
}
