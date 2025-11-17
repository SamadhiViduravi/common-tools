// Copyright (c) 2025 WSO2 LLC. (https://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import { RouterProvider, createBrowserRouter } from "react-router-dom";

import { useEffect, useState } from "react";

import Layout from "@layout/Layout";
import Error from "@layout/pages/404";
import MaintenancePage from "@layout/pages/Maintenance";
import ErrorHandler from "@component/common/ErrorHandler";
import PreLoader from "@component/common/PreLoader";
import { RootState, useAppSelector } from "@slices/store";

import { getActiveRoutesV2, routes } from "../route";

const AppHandler = () => {
  const [appState, setAppState] = useState<"loading" | "success" | "failed" | "maintenance">(
    "loading",
  );

  const auth = useAppSelector((state: RootState) => state.auth);
  const appConfig = useAppSelector((state: RootState) => state.appConfig);

  const router = createBrowserRouter([
    {
      path: "/",
      element: <Layout />,
      errorElement: <Error />,
      children: getActiveRoutesV2(routes, auth.roles),
    },
  ]);

  useEffect(() => {
    if (auth.status === "loading") {
      setAppState("loading");
    } else if (auth.status === "success") {
      setAppState("success");
    } else if (auth.status === "failed") {
      setAppState("failed");
    } else if (auth.mode === "maintenance") {
      setAppState("maintenance");
    }
  }, [auth.status, appConfig.state]);

  const renderApp = () => {
    switch (appState) {
      case "loading":
        return <PreLoader isLoading={true} message={"We are getting things ready ..."} />;

      case "failed":
        return <ErrorHandler message={auth.statusMessage} />;

      case "success":
        return <RouterProvider router={router} />;

      case "maintenance":
        return <MaintenancePage />;
    }
  };

  return <>{renderApp()}</>;
};

export default AppHandler;
