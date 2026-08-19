/**
 * @license
 * Copyright 2025 Canopy
 * SPDX-License-Identifier: Apache-2.0
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import type { DeviceAuthorizationData } from '@canopy-code/canopy-code-core';
import { useCanopyAuth } from './useCanopyAuth.js';
import {
  AuthType,
  canopyOAuth2Events,
  CanopyOAuth2Event,
} from '@canopy-code/canopy-code-core';

// Mock the canopyOAuth2Events
vi.mock('@canopy-code/canopy-code-core', async () => {
  const actual = await vi.importActual('@canopy-code/canopy-code-core');
  const mockEmitter = {
    on: vi.fn().mockReturnThis(),
    off: vi.fn().mockReturnThis(),
    emit: vi.fn().mockReturnThis(),
  };
  return {
    ...actual,
    canopyOAuth2Events: mockEmitter,
    CanopyOAuth2Event: {
      AuthUri: 'authUri',
      AuthProgress: 'authProgress',
    },
  };
});

const mockCanopyOAuth2Events = vi.mocked(canopyOAuth2Events);

describe('useCanopyAuth', () => {
  const mockDeviceAuth: DeviceAuthorizationData = {
    verification_uri: 'https://oauth.qwen.com/device',
    verification_uri_complete: 'https://oauth.qwen.com/device?user_code=ABC123',
    user_code: 'ABC123',
    expires_in: 1800,
    device_code: 'device_code_123',
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it('should initialize with default state when not Canopy auth', () => {
    const { result } = renderHook(() =>
      useCanopyAuth(AuthType.USE_GEMINI, false),
    );

    expect(result.current.canopyAuthState).toEqual({
      deviceAuth: null,
      authStatus: 'idle',
      authMessage: null,
    });
    expect(result.current.cancelCanopyAuth).toBeInstanceOf(Function);
  });

  it('should initialize with default state when Canopy auth but not authenticating', () => {
    const { result } = renderHook(() =>
      useCanopyAuth(AuthType.CANOPY_OAUTH, false),
    );

    expect(result.current.canopyAuthState).toEqual({
      deviceAuth: null,
      authStatus: 'idle',
      authMessage: null,
    });
    expect(result.current.cancelCanopyAuth).toBeInstanceOf(Function);
  });

  it('should set up event listeners when Canopy auth and authenticating', () => {
    renderHook(() => useCanopyAuth(AuthType.CANOPY_OAUTH, true));

    expect(mockCanopyOAuth2Events.on).toHaveBeenCalledWith(
      CanopyOAuth2Event.AuthUri,
      expect.any(Function),
    );
    expect(mockCanopyOAuth2Events.on).toHaveBeenCalledWith(
      CanopyOAuth2Event.AuthProgress,
      expect.any(Function),
    );
  });

  it('should handle device auth event', () => {
    let handleDeviceAuth: (deviceAuth: DeviceAuthorizationData) => void;

    mockCanopyOAuth2Events.on.mockImplementation((event, handler) => {
      if (event === CanopyOAuth2Event.AuthUri) {
        handleDeviceAuth = handler;
      }
      return mockCanopyOAuth2Events;
    });

    const { result } = renderHook(() =>
      useCanopyAuth(AuthType.CANOPY_OAUTH, true),
    );

    act(() => {
      handleDeviceAuth!(mockDeviceAuth);
    });

    expect(result.current.canopyAuthState.deviceAuth).toEqual(mockDeviceAuth);
    expect(result.current.canopyAuthState.authStatus).toBe('polling');
  });

  it('should handle auth progress event - success', () => {
    let handleAuthProgress: (
      status: 'success' | 'error' | 'polling' | 'timeout' | 'rate_limit',
      message?: string,
    ) => void;

    mockCanopyOAuth2Events.on.mockImplementation((event, handler) => {
      if (event === CanopyOAuth2Event.AuthProgress) {
        handleAuthProgress = handler;
      }
      return mockCanopyOAuth2Events;
    });

    const { result } = renderHook(() =>
      useCanopyAuth(AuthType.CANOPY_OAUTH, true),
    );

    act(() => {
      handleAuthProgress!('success', 'Authentication successful!');
    });

    expect(result.current.canopyAuthState.authStatus).toBe('success');
    expect(result.current.canopyAuthState.authMessage).toBe(
      'Authentication successful!',
    );
  });

  it('should handle auth progress event - error', () => {
    let handleAuthProgress: (
      status: 'success' | 'error' | 'polling' | 'timeout' | 'rate_limit',
      message?: string,
    ) => void;

    mockCanopyOAuth2Events.on.mockImplementation((event, handler) => {
      if (event === CanopyOAuth2Event.AuthProgress) {
        handleAuthProgress = handler;
      }
      return mockCanopyOAuth2Events;
    });

    const { result } = renderHook(() =>
      useCanopyAuth(AuthType.CANOPY_OAUTH, true),
    );

    act(() => {
      handleAuthProgress!('error', 'Authentication failed');
    });

    expect(result.current.canopyAuthState.authStatus).toBe('error');
    expect(result.current.canopyAuthState.authMessage).toBe(
      'Authentication failed',
    );
  });

  it('should handle auth progress event - polling', () => {
    let handleAuthProgress: (
      status: 'success' | 'error' | 'polling' | 'timeout' | 'rate_limit',
      message?: string,
    ) => void;

    mockCanopyOAuth2Events.on.mockImplementation((event, handler) => {
      if (event === CanopyOAuth2Event.AuthProgress) {
        handleAuthProgress = handler;
      }
      return mockCanopyOAuth2Events;
    });

    const { result } = renderHook(() =>
      useCanopyAuth(AuthType.CANOPY_OAUTH, true),
    );

    act(() => {
      handleAuthProgress!('polling', 'Waiting for user authorization...');
    });

    expect(result.current.canopyAuthState.authStatus).toBe('polling');
    expect(result.current.canopyAuthState.authMessage).toBe(
      'Waiting for user authorization...',
    );
  });

  it('should handle auth progress event - rate_limit', () => {
    let handleAuthProgress: (
      status: 'success' | 'error' | 'polling' | 'timeout' | 'rate_limit',
      message?: string,
    ) => void;

    mockCanopyOAuth2Events.on.mockImplementation((event, handler) => {
      if (event === CanopyOAuth2Event.AuthProgress) {
        handleAuthProgress = handler;
      }
      return mockCanopyOAuth2Events;
    });

    const { result } = renderHook(() =>
      useCanopyAuth(AuthType.CANOPY_OAUTH, true),
    );

    act(() => {
      handleAuthProgress!(
        'rate_limit',
        'Too many requests. The server is rate limiting our requests. Please select a different authentication method or try again later.',
      );
    });

    expect(result.current.canopyAuthState.authStatus).toBe('rate_limit');
    expect(result.current.canopyAuthState.authMessage).toBe(
      'Too many requests. The server is rate limiting our requests. Please select a different authentication method or try again later.',
    );
  });

  it('should handle auth progress event without message', () => {
    let handleAuthProgress: (
      status: 'success' | 'error' | 'polling' | 'timeout' | 'rate_limit',
      message?: string,
    ) => void;

    mockCanopyOAuth2Events.on.mockImplementation((event, handler) => {
      if (event === CanopyOAuth2Event.AuthProgress) {
        handleAuthProgress = handler;
      }
      return mockCanopyOAuth2Events;
    });

    const { result } = renderHook(() =>
      useCanopyAuth(AuthType.CANOPY_OAUTH, true),
    );

    act(() => {
      handleAuthProgress!('success');
    });

    expect(result.current.canopyAuthState.authStatus).toBe('success');
    expect(result.current.canopyAuthState.authMessage).toBe(null);
  });

  it('should clean up event listeners when auth type changes', () => {
    const { rerender } = renderHook(
      ({ pendingAuthType, isAuthenticating }) =>
        useCanopyAuth(pendingAuthType, isAuthenticating),
      {
        initialProps: {
          pendingAuthType: AuthType.CANOPY_OAUTH,
          isAuthenticating: true,
        },
      },
    );

    // Change to non-Canopy auth
    rerender({ pendingAuthType: AuthType.USE_GEMINI, isAuthenticating: true });

    expect(mockCanopyOAuth2Events.off).toHaveBeenCalledWith(
      CanopyOAuth2Event.AuthUri,
      expect.any(Function),
    );
    expect(mockCanopyOAuth2Events.off).toHaveBeenCalledWith(
      CanopyOAuth2Event.AuthProgress,
      expect.any(Function),
    );
  });

  it('should clean up event listeners when authentication stops', () => {
    const { rerender } = renderHook(
      ({ isAuthenticating }) =>
        useCanopyAuth(AuthType.CANOPY_OAUTH, isAuthenticating),
      { initialProps: { isAuthenticating: true } },
    );

    // Stop authentication
    rerender({ isAuthenticating: false });

    expect(mockCanopyOAuth2Events.off).toHaveBeenCalledWith(
      CanopyOAuth2Event.AuthUri,
      expect.any(Function),
    );
    expect(mockCanopyOAuth2Events.off).toHaveBeenCalledWith(
      CanopyOAuth2Event.AuthProgress,
      expect.any(Function),
    );
  });

  it('should clean up event listeners on unmount', () => {
    const { unmount } = renderHook(() =>
      useCanopyAuth(AuthType.CANOPY_OAUTH, true),
    );

    unmount();

    expect(mockCanopyOAuth2Events.off).toHaveBeenCalledWith(
      CanopyOAuth2Event.AuthUri,
      expect.any(Function),
    );
    expect(mockCanopyOAuth2Events.off).toHaveBeenCalledWith(
      CanopyOAuth2Event.AuthProgress,
      expect.any(Function),
    );
  });

  it('should reset state when switching from Canopy auth to another auth type', () => {
    let handleDeviceAuth: (deviceAuth: DeviceAuthorizationData) => void;

    mockCanopyOAuth2Events.on.mockImplementation((event, handler) => {
      if (event === CanopyOAuth2Event.AuthUri) {
        handleDeviceAuth = handler;
      }
      return mockCanopyOAuth2Events;
    });

    const { result, rerender } = renderHook(
      ({ pendingAuthType, isAuthenticating }) =>
        useCanopyAuth(pendingAuthType, isAuthenticating),
      {
        initialProps: {
          pendingAuthType: AuthType.CANOPY_OAUTH,
          isAuthenticating: true,
        },
      },
    );

    // Simulate device auth
    act(() => {
      handleDeviceAuth!(mockDeviceAuth);
    });

    expect(result.current.canopyAuthState.deviceAuth).toEqual(mockDeviceAuth);
    expect(result.current.canopyAuthState.authStatus).toBe('polling');

    // Switch to different auth type
    rerender({ pendingAuthType: AuthType.USE_GEMINI, isAuthenticating: true });

    expect(result.current.canopyAuthState.deviceAuth).toBe(null);
    expect(result.current.canopyAuthState.authStatus).toBe('idle');
    expect(result.current.canopyAuthState.authMessage).toBe(null);
  });

  it('should reset state when authentication stops', () => {
    let handleDeviceAuth: (deviceAuth: DeviceAuthorizationData) => void;

    mockCanopyOAuth2Events.on.mockImplementation((event, handler) => {
      if (event === CanopyOAuth2Event.AuthUri) {
        handleDeviceAuth = handler;
      }
      return mockCanopyOAuth2Events;
    });

    const { result, rerender } = renderHook(
      ({ isAuthenticating }) =>
        useCanopyAuth(AuthType.CANOPY_OAUTH, isAuthenticating),
      { initialProps: { isAuthenticating: true } },
    );

    // Simulate device auth
    act(() => {
      handleDeviceAuth!(mockDeviceAuth);
    });

    expect(result.current.canopyAuthState.deviceAuth).toEqual(mockDeviceAuth);
    expect(result.current.canopyAuthState.authStatus).toBe('polling');

    // Stop authentication
    rerender({ isAuthenticating: false });

    expect(result.current.canopyAuthState.deviceAuth).toBe(null);
    expect(result.current.canopyAuthState.authStatus).toBe('idle');
    expect(result.current.canopyAuthState.authMessage).toBe(null);
  });

  it('should handle cancelCanopyAuth function', () => {
    let handleDeviceAuth: (deviceAuth: DeviceAuthorizationData) => void;

    mockCanopyOAuth2Events.on.mockImplementation((event, handler) => {
      if (event === CanopyOAuth2Event.AuthUri) {
        handleDeviceAuth = handler;
      }
      return mockCanopyOAuth2Events;
    });

    const { result } = renderHook(() =>
      useCanopyAuth(AuthType.CANOPY_OAUTH, true),
    );

    // Set up some state
    act(() => {
      handleDeviceAuth!(mockDeviceAuth);
    });

    expect(result.current.canopyAuthState.deviceAuth).toEqual(mockDeviceAuth);

    // Cancel auth
    act(() => {
      result.current.cancelCanopyAuth();
    });

    expect(result.current.canopyAuthState.deviceAuth).toBe(null);
    expect(result.current.canopyAuthState.authStatus).toBe('idle');
    expect(result.current.canopyAuthState.authMessage).toBe(null);
  });

  it('should handle different auth types correctly', () => {
    // Test with Canopy OAuth - should set up event listeners when authenticating
    const { result: canopyResult } = renderHook(() =>
      useCanopyAuth(AuthType.CANOPY_OAUTH, true),
    );
    expect(canopyResult.current.canopyAuthState.authStatus).toBe('idle');
    expect(mockCanopyOAuth2Events.on).toHaveBeenCalled();

    // Test with other auth types - should not set up event listeners
    const { result: geminiResult } = renderHook(() =>
      useCanopyAuth(AuthType.USE_GEMINI, true),
    );
    expect(geminiResult.current.canopyAuthState.authStatus).toBe('idle');

    const { result: oauthResult } = renderHook(() =>
      useCanopyAuth(AuthType.USE_OPENAI, true),
    );
    expect(oauthResult.current.canopyAuthState.authStatus).toBe('idle');
  });

  it('should initialize with idle status when starting authentication with Canopy auth', () => {
    const { result } = renderHook(() =>
      useCanopyAuth(AuthType.CANOPY_OAUTH, true),
    );

    expect(result.current.canopyAuthState.authStatus).toBe('idle');
    expect(mockCanopyOAuth2Events.on).toHaveBeenCalled();
  });
});
