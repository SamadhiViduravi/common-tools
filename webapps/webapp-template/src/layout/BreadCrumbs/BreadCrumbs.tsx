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

import { Tooltip, Box, Typography, useTheme } from "@mui/material";
import Breadcrumbs from "@mui/material/Breadcrumbs";
import { useSelector } from "react-redux";

import { Link, useLocation } from "react-router-dom";

import { getActiveParentRoutes, routes } from "@src/route";
import { selectRoles } from "@slices/authSlice/auth";

import * as React from "react";

const MAX_LENGTH = 5;

function handleClick(event: React.MouseEvent<HTMLDivElement, MouseEvent>) {
  event.preventDefault();
}

export default function BasicBreadcrumbs() {
  const location = useLocation();
  const roles = useSelector(selectRoles);
  const theme = useTheme();

  const activeParentPaths = getActiveParentRoutes({ routes, roles }).map((route) => {
    if (route === "/") return "/";
    return route.slice(1);
  });
  const { pathname } = location;
  const pathnames = ["", ...pathname.split("/").filter(Boolean)];

  return (
    <Box
      role="presentation"
      sx={{ mb: -0.125 }}
      onClick={handleClick}
    >
      <Breadcrumbs
        aria-label="breadcrumb"
        sx={{
          "& .MuiBreadcrumbs-separator": {
            marginLeft: theme.spacing(0.5),
            marginRight: theme.spacing(0.5),
            fontSize: "12px",
            lineHeight: 1,
          },
        }}
      >
        {pathnames.map((path, index) => {
          // Build the route to this breadcrumb
          const routeTo = `${pathnames
            .slice(0, index + 1)
            .filter(Boolean)
            .map((seg) => seg.trim())
            .join("/")}`;

          const isLast = index === pathnames.length - 1;

          // Check if the path is an active path or not
          const isActive = activeParentPaths.includes(path);

          const isLong = path.length >= MAX_LENGTH;
          const label =
            !isLast && isLong ? (
              <Typography variant="body2" sx={{ fontSize: "12px" }}>
                {path.slice(0, 4)}...
              </Typography>
            ) : (
              <Typography variant="body2" sx={{ fontSize: "12px" }}>
                {path}
              </Typography>
            );

          return isActive && !isLast ? (
            <Tooltip key={path} title={isLong ? path : ""} placement="bottom">
              <Box
                component={Link}
                to={routeTo}
                sx={{
                  textDecoration: "none",
                  color: theme.palette.textTokens[200],
                  padding: theme.spacing(0.5),
                  borderRadius: "4px",
                  transition: "all 0.2s",
                  display: "flex",
                  alignItems: "center",
                  "&:hover": {
                    color: theme.palette.textTokens[100],
                    backgroundColor: theme.palette.background.mainLight,
                  },
                }}
              >
                {label}
              </Box>
            </Tooltip>
          ) : (
            <Box
              key={path}
              sx={{
                textDecoration: "none",
                color: theme.palette.textTokens[300],
                padding: theme.spacing(0.5),
                display: "flex",
                alignItems: "center",
              }}
            >
              {label}
            </Box>
          );
        })}
      </Breadcrumbs>
    </Box>
  );
}
