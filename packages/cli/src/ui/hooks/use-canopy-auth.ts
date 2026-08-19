/**
 * @license
 * Copyright 2025 Canopy
 * SPDX-License-Identifier: Apache-2.0
 */

import { useState, useCallback, useEffect } from 'react';
import {
  AuthType,
  canopyOAuth2Events,
  CanopyOAuth2Event,
  type DeviceAuthorizationData,
} from '@canopy-code/canopy-code-core';

export interface CanopyAuthState {
  deviceAuth: DeviceAuthorizationData | null;
  authStatus:
    | 'idle'
    | 'polling'
    | 'success'
    | 'error'
    | 'timeout'
    | 'rate_limit';
  authMessage: string | null;
}

export interface ExternalAuthState {
  title: string;
  message: string;
  detail?: string;
}

export const useCanopyAuth = (
  pendingAuthType: AuthType | undefined,
  isAuthenticating: boolean,
) => {
  const [canopyAuthState, setCanopyAuthState] = useState<CanopyAuthState>({
    deviceAuth: null,
    authStatus: 'idle',
    authMessage: null,
  });

  const isCanopyAuth = pendingAuthType === AuthType.CANOPY_OAUTH;

  // Set up event listeners when authentication starts
  useEffect(() => {
    if (!isCanopyAuth || !isAuthenticating) {
      // Reset state when not authenticating or not Canopy auth
      setCanopyAuthState({
        deviceAuth: null,
        authStatus: 'idle',
        authMessage: null,
      });
      return;
    }

    setCanopyAuthState((prev) => ({
      ...prev,
      authStatus: 'idle',
    }));

    // Set up event listeners
    const handleDeviceAuth = (deviceAuth: DeviceAuthorizationData) => {
      setCanopyAuthState((prev) => ({
        ...prev,
        deviceAuth: {
          verification_uri: deviceAuth.verification_uri,
          verification_uri_complete: deviceAuth.verification_uri_complete,
          user_code: deviceAuth.user_code,
          expires_in: deviceAuth.expires_in,
          device_code: deviceAuth.device_code,
        },
        authStatus: 'polling',
      }));
    };

    const handleAuthProgress = (
      status: 'success' | 'error' | 'polling' | 'timeout' | 'rate_limit',
      message?: string,
    ) => {
      setCanopyAuthState((prev) => ({
        ...prev,
        authStatus: status,
        authMessage: message || null,
      }));
    };

    // Add event listeners
    canopyOAuth2Events.on(CanopyOAuth2Event.AuthUri, handleDeviceAuth);
    canopyOAuth2Events.on(CanopyOAuth2Event.AuthProgress, handleAuthProgress);

    // Cleanup event listeners when component unmounts or auth finishes
    return () => {
      canopyOAuth2Events.off(CanopyOAuth2Event.AuthUri, handleDeviceAuth);
      canopyOAuth2Events.off(
        CanopyOAuth2Event.AuthProgress,
        handleAuthProgress,
      );
    };
  }, [isCanopyAuth, isAuthenticating]);

  const cancelCanopyAuth = useCallback(() => {
    // Emit cancel event to stop polling
    canopyOAuth2Events.emit(CanopyOAuth2Event.AuthCancel);

    setCanopyAuthState({
      deviceAuth: null,
      authStatus: 'idle',
      authMessage: null,
    });
  }, []);

  return {
    canopyAuthState,
    cancelCanopyAuth,
  };
};
