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

import { Box, Stack, Tooltip, Typography, useTheme } from "@mui/material";
import { ChevronLeft, ChevronRight, Moon, Sun } from "lucide-react";

import { useMemo, useState } from "react";

import pJson from "@root/package.json";
import type { NavState } from "@/types/types";
import SidebarNavItem from "@component/layout/SidebarNavItem";
import { ColorModeContext } from "@src/App";
import { getActiveRouteDetails } from "@src/route";

interface SidebarProps {
  open: boolean;
  handleDrawer: () => void;
  roles: string[];
  currentPath: string;
}

const Sidebar = (props: SidebarProps) => {
  const allRoutes = useMemo(() => getActiveRouteDetails(props.roles), [props.roles]);

  // Single state object for nav state
  const [navState, setNavState] = useState<NavState>({
    hovered: null,
    active: null,
    expanded: null,
  });

  // Handlers
  const handleClick = (idx: number) => {
    setNavState((prev) => ({
      ...prev,
      active: idx,
      expanded: prev.expanded === idx ? null : idx,
    }));
  };

  const handleMouseEnter = (idx: number) => {
    setNavState((prev) => ({ ...prev, hovered: idx }));
  };

  const handleMouseLeave = () => {
    setNavState((prev) => ({ ...prev, hovered: null }));
  };
  const theme = useTheme();

  return (
    <ColorModeContext.Consumer>
      {(colorMode) => {
        return (
          <Box
            sx={{
              height: "100%",
              padding: theme.spacing(1),
              backgroundColor: theme.palette.background.secondary,
              zIndex: 10,
              display: "flex",
              flexDirection: "column",
              width: "fit-content",
              overflow: "visible",
            }}
          >
            {/* Navigation List */}
            <Stack
              direction="column"
              gap={1}
              sx={{
                overflow: "visible",
                width: props.open ? "100%" : "fit-content",
              }}
            >
              {allRoutes.map(
                (route, idx) =>
                  !route.bottomNav && (
                    <Box
                      key={idx}
                      onMouseEnter={() => handleMouseEnter(idx)}
                      onMouseLeave={handleMouseLeave}
                      sx={{
                        width: props.open ? "100%" : "fit-content",
                        cursor: props.open ? "pointer" : "default",
                      }}
                    >
                      <SidebarNavItem
                        route={route}
                        open={props.open}
                        isActive={navState.active === idx}
                        isHovered={navState.hovered === idx}
                        isExpanded={navState.expanded === idx}
                        onClick={() => handleClick(idx)}
                      />
                    </Box>
                  ),
              )}
            </Stack>

            {/* Spacer */}
            <Box sx={{ flex: 1 }} />

            {/* Footer */}
            <Box
              sx={{
                position: "relative",
                left: 0,
                width: "100%",
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
              }}
            >
              <Stack gap={1} sx={{ width: "100%" }}>
                {/* Bottom Navigation Items */}
                {allRoutes.map(
                  (route, idx) =>
                    route.bottomNav && (
                      <Box
                        key={idx}
                        sx={{
                          width: props.open ? "100%" : "fit-content",
                        }}
                      >
                        <SidebarNavItem
                          route={route}
                          open={props.open}
                          isActive={navState.active === idx}
                          isHovered={navState.hovered === idx}
                          isExpanded={navState.expanded === idx}
                          onClick={() => handleClick(idx)}
                        />
                      </Box>
                    ),
                )}

                {/* Control Buttons */}
                <Stack direction="column" gap={1} sx={{ pl: "2px", width: "100%" }}>
                  {/* Theme Toggle Button */}
                  <Tooltip
                    title={`Switch to ${colorMode.mode === "dark" ? "light" : "dark"} mode`}
                    placement="right"
                    arrow
                    disableHoverListener={props.open}
                    slotProps={{
                      tooltip: {
                        sx: {
                          backgroundColor: theme.palette.neutral[10],
                          color: theme.palette.neutral.white,
                          padding: theme.spacing(0.75, 1),
                          borderRadius: "4px",
                          fontSize: "14px",
                          boxShadow: theme.shadows[8],
                        },
                      },
                      arrow: {
                        sx: {
                          color: theme.palette.neutral[10],
                        },
                      },
                    }}
                  >
                    <Box
                      component="button"
                      onClick={colorMode.toggleColorMode}
                      sx={{
                        width: "fit-content",
                        padding: theme.spacing(1),
                        borderRadius: "8px",
                        cursor: "pointer",
                        border: "none",
                        background: "none",
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "center",
                        gap: theme.spacing(1),
                        color: theme.palette.navigation.link,
                        transition: "all 0.2s",
                        "&:hover": {
                          backgroundColor: theme.palette.navigation.hoverBg,
                          color: theme.palette.navigation.hover,
                        },
                      }}
                    >
                      {colorMode.mode === "dark" ? <Sun size={18} /> : <Moon size={18} />}
                    </Box>
                  </Tooltip>

                  {/* Sidebar Toggle Button */}
                  <Tooltip
                    title={props.open ? "Collapse Sidebar" : "Expand Sidebar"}
                    placement="right"
                    arrow
                    disableHoverListener={props.open}
                    slotProps={{
                      tooltip: {
                        sx: {
                          backgroundColor: theme.palette.neutral[10],
                          color: theme.palette.neutral.white,
                          padding: theme.spacing(0.75, 1),
                          borderRadius: "4px",
                          fontSize: "14px",
                          boxShadow: theme.shadows[8],
                        },
                      },
                      arrow: {
                        sx: {
                          color: theme.palette.neutral[10],
                        },
                      },
                    }}
                  >
                    <Box
                      component="button"
                      onClick={props.handleDrawer}
                      sx={{
                        width: props.open ? "fit-content" : "fit-content",
                        padding: theme.spacing(1),
                        borderRadius: "8px",
                        cursor: "pointer",
                        border: "none",
                        background: "none",
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "center",
                        gap: theme.spacing(1),
                        color: theme.palette.navigation.link,
                        transition: "all 0.2s",
                        "&:hover": {
                          backgroundColor: theme.palette.navigation.hoverBg,
                          color: theme.palette.navigation.hover,
                        },
                      }}
                    >
                      {!props.open ? <ChevronRight size={20} /> : <ChevronLeft size={20} />}
                    </Box>
                  </Tooltip>

                  {/* Version Info Button */}
                  <Tooltip
                    title={`v ${pJson.version} | © ${new Date().getFullYear()} WSO2 LLC`}
                    placement="right"
                    arrow
                    disableHoverListener={props.open}
                    slotProps={{
                      tooltip: {
                        sx: {
                          backgroundColor: theme.palette.neutral[10],
                          color: theme.palette.neutral.white,
                          padding: theme.spacing(0.75, 1),
                          borderRadius: "4px",
                          fontSize: "14px",
                          boxShadow: theme.shadows[8],
                          whiteSpace: "nowrap",
                        },
                      },
                      arrow: {
                        sx: {
                          color: theme.palette.neutral[10],
                        },
                      },
                    }}
                  >
                    <Box
                      sx={{
                        width: "100%",
                        padding: theme.spacing(1),
                        borderRadius: "8px",
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "center",
                        textAlign: "center",
                        color: theme.palette.navigation.link,
                        transition: "all 0.2s",
                        cursor: "pointer",
                        "&:hover": {
                          backgroundColor: theme.palette.navigation.hoverBg,
                          color: theme.palette.navigation.hover,
                        },
                      }}
                    >
                      {!props.open ? (
                        <Typography variant="body2">v1</Typography>
                      ) : (
                        <Typography variant="body2">
                          v {pJson.version} | © {new Date().getFullYear()} WSO2 LLC
                        </Typography>
                      )}
                    </Box>
                  </Tooltip>
                </Stack>
              </Stack>
            </Box>
          </Box>
        );
      }}
    </ColorModeContext.Consumer>
  );
};

export default Sidebar;
