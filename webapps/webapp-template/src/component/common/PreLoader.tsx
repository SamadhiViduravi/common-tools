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
import { Box } from "@mui/material";
import CircularProgress, {
  CircularProgressProps,
  circularProgressClasses,
} from "@mui/material/CircularProgress";

import LoadingLogo from "@assets/images/loading.svg";
import Wso2Logo from "@assets/images/wso2-logo.svg";
import StateWithImage from "@component/ui/StateWithImage";
import { APP_NAME } from "@config/config";

function CustomCircularProgress(props: CircularProgressProps) {
  return (
    <Box sx={{ position: "relative" }}>
      <CircularProgress
        variant="determinate"
        sx={{
          color: (theme) => theme.palette.grey[800],
        }}
        size={40}
        thickness={4}
        {...props}
        value={100}
      />
      <CircularProgress
        variant="indeterminate"
        disableShrink
        sx={{
          color: (theme) => theme.palette.primary.main,
          animationDuration: "550ms",
          position: "absolute",
          left: 0,
          [`& .${circularProgressClasses.circle}`]: {
            strokeLinecap: "round",
          },
        }}
        size={40}
        thickness={4}
        {...props}
      />
    </Box>
  );
}

interface PreLoaderProps {
  message?: string;
  hideLogo?: boolean;
  isLoading?: boolean;
}

const PreLoader = (props: PreLoaderProps) => {
  const { message = `Loading ${APP_NAME} Data... `, hideLogo, isLoading } = props;
  return (
    <div className="w-full h-full flex justify-center items-center">
      <div className="p-12 w-120 py-10 rounded-2xl shadow-md bg-st-bg-main-light">
        <div className="flex flex-col items-center justify-center gap-4">
          {!hideLogo && (
            <img
              alt="logo"
              width={150}
              height={150}
              className="h-auto w-[150px] select-none"
              src={Wso2Logo}
            />
          )}

          <StateWithImage message={message} imageUrl={LoadingLogo} />

          {isLoading && <CustomCircularProgress size={40} thickness={4} />}
        </div>
      </div>
    </div>
  );
};

export default PreLoader;
