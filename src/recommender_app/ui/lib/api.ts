import { useQuery, useSuspenseQuery, useMutation } from "@tanstack/react-query";
import type { UseQueryOptions, UseSuspenseQueryOptions, UseMutationOptions } from "@tanstack/react-query";
export class ApiError extends Error {
    status: number;
    statusText: string;
    body: unknown;
    constructor(status: number, statusText: string, body: unknown){
        super(`HTTP ${status}: ${statusText}`);
        this.name = "ApiError";
        this.status = status;
        this.statusText = statusText;
        this.body = body;
    }
}
export interface HTTPValidationError {
    detail?: ValidationError[];
}
export interface Product {
    base_price: number;
    category: string;
    product_id: string;
    product_name: string;
    product_slug: string;
}
export interface RecommendRequest {
    cart?: string[];
    mode: "known" | "anonymous";
    user_id?: string | null;
}
export interface RecommendResponse {
    mode: string;
    recommendations: Recommendation[];
    source: string;
}
export interface Recommendation {
    product: string;
    rank: number;
    score: number;
}
export interface UserInfo {
    primary_store?: string | null;
    total_orders?: number | null;
    user_id: string;
}
export interface UserProfile {
    primary_store?: string | null;
    store_visits?: number | null;
    total_orders?: number | null;
    user_id: string;
}
export interface ValidationError {
    ctx?: Record<string, unknown>;
    input?: unknown;
    loc: (string | number)[];
    msg: string;
    type: string;
}
export const health_api_health_get = async (options?: RequestInit): Promise<{
    data: unknown;
}> =>{
    const res = await fetch("/api/health", {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const health_api_health_getKey = ()=>{
    return [
        "/api/health"
    ] as const;
};
export function useHealth_api_health_get<TData = {
    data: unknown;
}>(options?: {
    query?: Omit<UseQueryOptions<{
        data: unknown;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: health_api_health_getKey(),
        queryFn: ()=>health_api_health_get(),
        ...options?.query
    });
}
export function useHealth_api_health_getSuspense<TData = {
    data: unknown;
}>(options?: {
    query?: Omit<UseSuspenseQueryOptions<{
        data: unknown;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: health_api_health_getKey(),
        queryFn: ()=>health_api_health_get(),
        ...options?.query
    });
}
export const list_products_api_products_get = async (options?: RequestInit): Promise<{
    data: Product[];
}> =>{
    const res = await fetch("/api/products", {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const list_products_api_products_getKey = ()=>{
    return [
        "/api/products"
    ] as const;
};
export function useList_products_api_products_get<TData = {
    data: Product[];
}>(options?: {
    query?: Omit<UseQueryOptions<{
        data: Product[];
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: list_products_api_products_getKey(),
        queryFn: ()=>list_products_api_products_get(),
        ...options?.query
    });
}
export function useList_products_api_products_getSuspense<TData = {
    data: Product[];
}>(options?: {
    query?: Omit<UseSuspenseQueryOptions<{
        data: Product[];
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: list_products_api_products_getKey(),
        queryFn: ()=>list_products_api_products_get(),
        ...options?.query
    });
}
export const recommend_api_recommend_post = async (data: RecommendRequest, options?: RequestInit): Promise<{
    data: RecommendResponse;
}> =>{
    const res = await fetch("/api/recommend", {
        ...options,
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            ...options?.headers
        },
        body: JSON.stringify(data)
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useRecommend_api_recommend_post(options?: {
    mutation?: UseMutationOptions<{
        data: RecommendResponse;
    }, ApiError, RecommendRequest>;
}) {
    return useMutation({
        mutationFn: (data)=>recommend_api_recommend_post(data),
        ...options?.mutation
    });
}
export interface List_users_api_users_getParams {
    limit?: number;
}
export const list_users_api_users_get = async (params?: List_users_api_users_getParams, options?: RequestInit): Promise<{
    data: UserInfo[];
}> =>{
    const searchParams = new URLSearchParams();
    if (params?.limit != null) searchParams.set("limit", String(params?.limit));
    const queryString = searchParams.toString();
    const url = queryString ? `/api/users?${queryString}` : "/api/users";
    const res = await fetch(url, {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const list_users_api_users_getKey = (params?: List_users_api_users_getParams)=>{
    return [
        "/api/users",
        params
    ] as const;
};
export function useList_users_api_users_get<TData = {
    data: UserInfo[];
}>(options?: {
    params?: List_users_api_users_getParams;
    query?: Omit<UseQueryOptions<{
        data: UserInfo[];
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: list_users_api_users_getKey(options?.params),
        queryFn: ()=>list_users_api_users_get(options?.params),
        ...options?.query
    });
}
export function useList_users_api_users_getSuspense<TData = {
    data: UserInfo[];
}>(options?: {
    params?: List_users_api_users_getParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: UserInfo[];
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: list_users_api_users_getKey(options?.params),
        queryFn: ()=>list_users_api_users_get(options?.params),
        ...options?.query
    });
}
export interface Get_user_api_users__user_id__getParams {
    user_id: string;
}
export const get_user_api_users__user_id__get = async (params: Get_user_api_users__user_id__getParams, options?: RequestInit): Promise<{
    data: UserProfile;
}> =>{
    const res = await fetch(`/api/users/${params.user_id}`, {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const get_user_api_users__user_id__getKey = (params?: Get_user_api_users__user_id__getParams)=>{
    return [
        "/api/users/{user_id}",
        params
    ] as const;
};
export function useGet_user_api_users__user_id__get<TData = {
    data: UserProfile;
}>(options: {
    params: Get_user_api_users__user_id__getParams;
    query?: Omit<UseQueryOptions<{
        data: UserProfile;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: get_user_api_users__user_id__getKey(options.params),
        queryFn: ()=>get_user_api_users__user_id__get(options.params),
        ...options?.query
    });
}
export function useGet_user_api_users__user_id__getSuspense<TData = {
    data: UserProfile;
}>(options: {
    params: Get_user_api_users__user_id__getParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: UserProfile;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: get_user_api_users__user_id__getKey(options.params),
        queryFn: ()=>get_user_api_users__user_id__get(options.params),
        ...options?.query
    });
}
export interface Spa_fallback__path__getParams {
    path: string;
}
export const spa_fallback__path__get = async (params: Spa_fallback__path__getParams, options?: RequestInit): Promise<{
    data: unknown;
}> =>{
    const res = await fetch(`/${params.path}`, {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const spa_fallback__path__getKey = (params?: Spa_fallback__path__getParams)=>{
    return [
        "/{path}",
        params
    ] as const;
};
export function useSpa_fallback__path__get<TData = {
    data: unknown;
}>(options: {
    params: Spa_fallback__path__getParams;
    query?: Omit<UseQueryOptions<{
        data: unknown;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: spa_fallback__path__getKey(options.params),
        queryFn: ()=>spa_fallback__path__get(options.params),
        ...options?.query
    });
}
export function useSpa_fallback__path__getSuspense<TData = {
    data: unknown;
}>(options: {
    params: Spa_fallback__path__getParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: unknown;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: spa_fallback__path__getKey(options.params),
        queryFn: ()=>spa_fallback__path__get(options.params),
        ...options?.query
    });
}
